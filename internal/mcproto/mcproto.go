// Package mcproto implements the few pieces of the Minecraft Java protocol
// that lcm needs before a connection is encrypted or compressed: VarInts,
// uncompressed packet framing, the Handshake, Login Start, the status
// exchange (server list ping), Login Disconnect, the no-op Set Compression
// keepalive and the legacy (pre-1.7) ping reply. See docs/on-demand.md.
package mcproto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

var (
	// ErrShort means the buffer ends inside a VarInt or packet; read more.
	ErrShort = errors.New("mcproto: incomplete data")
	// ErrMalformed is returned for anything that cannot be valid Minecraft framing.
	ErrMalformed = errors.New("mcproto: malformed packet")
)

// Handshake intents (next state).
const (
	IntentStatus   = 1
	IntentLogin    = 2
	IntentTransfer = 3
)

// Protocol47 is 1.8, the first version with Login Set Compression.
const Protocol47 = 47

// SetCompressionNoop is the clientbound Login "Set Compression" packet
// (id 0x03) with threshold -1: length 6, id 0x03, VarInt -1 (5 bytes). With
// compression off, the client treats it as a no-op and sends no reply, so it
// can keep a client in the login phase alive (1.8 to at least 26.3).
var SetCompressionNoop = []byte{0x06, 0x03, 0xff, 0xff, 0xff, 0xff, 0x0f}

// AppendVarInt appends v in Minecraft VarInt encoding (two's complement, 1-5 bytes).
func AppendVarInt(b []byte, v int32) []byte {
	u := uint32(v)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

// VarIntLen is the encoded size of v.
func VarIntLen(v int32) int { return len(AppendVarInt(make([]byte, 0, 5), v)) }

// ReadVarInt decodes a VarInt from the front of b, returning the value and
// the number of bytes used. It returns ErrShort if b ends inside the VarInt
// and ErrMalformed if it is longer than 5 bytes.
func ReadVarInt(b []byte) (int32, int, error) {
	var u uint32
	for i := 0; i < 5; i++ {
		if i >= len(b) {
			return 0, 0, ErrShort
		}
		c := b[i]
		if i == 4 && c&0xf0 != 0 {
			return 0, 0, fmt.Errorf("%w: VarInt too long", ErrMalformed)
		}
		u |= uint32(c&0x7f) << (7 * i)
		if c&0x80 == 0 {
			return int32(u), i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("%w: VarInt too long", ErrMalformed)
}

// AppendString appends a VarInt-length-prefixed UTF-8 string.
func AppendString(b []byte, s string) []byte {
	b = AppendVarInt(b, int32(len(s)))
	return append(b, s...)
}

// AppendPacket appends an uncompressed packet: VarInt length, VarInt id, body.
func AppendPacket(b []byte, id int32, body []byte) []byte {
	b = AppendVarInt(b, int32(VarIntLen(id)+len(body)))
	b = AppendVarInt(b, id)
	return append(b, body...)
}

// ParsePacket splits one uncompressed packet off the front of b. total is
// the number of bytes it occupies. max bounds the declared length.
func ParsePacket(b []byte, max int) (id int32, body []byte, total int, err error) {
	n, k, err := ReadVarInt(b)
	if err != nil {
		if errors.Is(err, ErrShort) && len(b) >= 3 {
			// A length needing more than 3 bytes exceeds any max we use.
			return 0, nil, 0, fmt.Errorf("%w: packet too long", ErrMalformed)
		}
		return 0, nil, 0, err
	}
	if n <= 0 || int(n) > max {
		return 0, nil, 0, fmt.Errorf("%w: packet length %d", ErrMalformed, n)
	}
	if len(b) < k+int(n) {
		return 0, nil, 0, ErrShort
	}
	p := b[k : k+int(n)]
	id, j, err := ReadVarInt(p)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("%w: packet id", ErrMalformed)
	}
	return id, p[j:], k + int(n), nil
}

// field reads successive fields from a packet body.
type field struct {
	b   []byte
	err error
}

func (f *field) varint() int32 {
	if f.err != nil {
		return 0
	}
	v, n, err := ReadVarInt(f.b)
	if err != nil {
		f.err = fmt.Errorf("%w: field", ErrMalformed)
		return 0
	}
	f.b = f.b[n:]
	return v
}

func (f *field) str(maxBytes int) string {
	n := f.varint()
	if f.err != nil {
		return ""
	}
	if n < 0 || int(n) > maxBytes || int(n) > len(f.b) {
		f.err = fmt.Errorf("%w: string length %d", ErrMalformed, n)
		return ""
	}
	s := string(f.b[:n])
	f.b = f.b[n:]
	if !utf8.ValidString(s) {
		f.err = fmt.Errorf("%w: string is not UTF-8", ErrMalformed)
	}
	return s
}

func (f *field) u16() uint16 {
	if f.err != nil {
		return 0
	}
	if len(f.b) < 2 {
		f.err = fmt.Errorf("%w: short field", ErrMalformed)
		return 0
	}
	v := binary.BigEndian.Uint16(f.b)
	f.b = f.b[2:]
	return v
}

// Handshake is the first serverbound packet (id 0x00) of every modern connection.
type Handshake struct {
	Protocol int32
	// Host is the address the client typed, as sent. Forge appends markers
	// such as "\x00FML\x00"; BungeeCord IP forwarding appends more fields.
	Host   string
	Port   uint16
	Intent int32
}

// maxHostBytes allows BungeeCord-style forwarding data in the host field.
const maxHostBytes = 32 << 10

// ParseHandshake parses a Handshake body (after the packet id).
func ParseHandshake(body []byte) (Handshake, error) {
	f := field{b: body}
	h := Handshake{Protocol: f.varint(), Host: f.str(maxHostBytes), Port: f.u16(), Intent: f.varint()}
	if f.err != nil {
		return Handshake{}, f.err
	}
	if len(f.b) != 0 {
		return Handshake{}, fmt.Errorf("%w: %d trailing bytes in handshake", ErrMalformed, len(f.b))
	}
	return h, nil
}

// AppendHandshake appends the framed Handshake packet.
func AppendHandshake(b []byte, h Handshake) []byte {
	body := AppendVarInt(nil, h.Protocol)
	body = AppendString(body, h.Host)
	body = binary.BigEndian.AppendUint16(body, h.Port)
	body = AppendVarInt(body, h.Intent)
	return AppendPacket(b, 0x00, body)
}

// CleanHost strips Forge/FML markers and forwarding data (everything from
// the first NUL) and a trailing dot.
func CleanHost(host string) string {
	if i := strings.IndexByte(host, 0); i >= 0 {
		host = host[:i]
	}
	return strings.TrimSuffix(host, ".")
}

// ParseLoginStartName returns the player name from a Login Start body. Only
// the leading name field is read; later fields differ between versions.
func ParseLoginStartName(body []byte) (string, error) {
	f := field{b: body}
	name := f.str(16 * 4)
	return name, f.err
}

// AppendLoginStart appends a framed Login Start with just a name (the
// 1.8-1.18 layout; enough for tests).
func AppendLoginStart(b []byte, name string) []byte {
	return AppendPacket(b, 0x00, AppendString(nil, name))
}

// TextJSON returns a JSON text component {"text": s}.
func TextJSON(s string) string {
	b, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{s})
	return string(b)
}

// LoginDisconnect returns the framed clientbound Login Disconnect packet
// (id 0x00; the reason is a JSON text component in every version).
func LoginDisconnect(msg string) []byte {
	return AppendPacket(nil, 0x00, AppendString(nil, TextJSON(msg)))
}

// StatusDoc is the server list ping response document.
type StatusDoc struct {
	Version struct {
		Name     string `json:"name"`
		Protocol int32  `json:"protocol"`
	} `json:"version"`
	Players struct {
		Max    int   `json:"max"`
		Online int   `json:"online"`
		Sample []any `json:"sample"`
	} `json:"players"`
	Description struct {
		Text string `json:"text"`
	} `json:"description"`
}

// NewStatus builds a status document with the given MOTD, version name and protocol.
func NewStatus(motd, version string, protocol int32) StatusDoc {
	var d StatusDoc
	d.Version.Name = version
	d.Version.Protocol = protocol
	d.Players.Sample = []any{}
	d.Description.Text = motd
	return d
}

// StatusResponse returns the framed Status Response packet (id 0x00).
func StatusResponse(doc any) []byte {
	j, _ := json.Marshal(doc)
	return AppendPacket(nil, 0x00, AppendString(nil, string(j)))
}

// StatusRequest is the framed serverbound Status Request (id 0x00, empty).
var StatusRequest = []byte{0x01, 0x00}

// ParseStatusResponse returns the JSON string of a Status Response body.
func ParseStatusResponse(body []byte) (string, error) {
	f := field{b: body}
	s := f.str(1 << 20)
	return s, f.err
}

// LegacyKick returns the pre-1.7 server list reply (the 1.4-1.6 "§1" format,
// sent as a 0xFF kick packet with a UTF-16BE string). Older clients show the
// fields less nicely but do not crash.
func LegacyKick(protocol int, version, motd string, online, max int) []byte {
	s := strings.Join([]string{"§1", strconv.Itoa(protocol), version, motd,
		strconv.Itoa(online), strconv.Itoa(max)}, "\x00")
	u := utf16.Encode([]rune(s))
	b := []byte{0xff}
	b = binary.BigEndian.AppendUint16(b, uint16(len(u)))
	for _, c := range u {
		b = binary.BigEndian.AppendUint16(b, c)
	}
	return b
}

// IsLegacyPing reports whether the first bytes of a connection are a pre-1.7
// server list ping, using the same rule as the vanilla server: 0xFE alone,
// 0xFE 0x01 alone, or 0xFE 0x01 0xFA. (A modern packet of length 254+ also
// starts with 0xFE 0x01 but is followed by packet id 0x00.) prefix may be
// shorter than 3 bytes when the client sent no more.
func IsLegacyPing(prefix []byte) bool {
	if len(prefix) == 0 || prefix[0] != 0xfe {
		return false
	}
	if len(prefix) == 1 {
		return true
	}
	if prefix[1] != 0x01 {
		return false
	}
	return len(prefix) == 2 || prefix[2] == 0xfa
}

// Reader reads packets from r and keeps every byte it has read, so the
// exact prefix can be replayed to the real server.
type Reader struct {
	r   io.Reader
	buf []byte
	off int // start of unparsed data
	max int
}

// NewReader returns a Reader whose packets may be at most max bytes long.
func NewReader(r io.Reader, max int) *Reader { return &Reader{r: r, max: max} }

// Raw returns every byte read so far (parsed or not).
func (r *Reader) Raw() []byte { return r.buf }

// Buffered returns the bytes read but not yet parsed.
func (r *Reader) Buffered() []byte { return r.buf[r.off:] }

// Fill reads once more from the underlying reader.
func (r *Reader) Fill() error {
	if len(r.buf) > r.max+16 {
		return fmt.Errorf("%w: too much data", ErrMalformed)
	}
	var tmp [4096]byte
	n, err := r.r.Read(tmp[:])
	r.buf = append(r.buf, tmp[:n]...)
	if n > 0 {
		return nil
	}
	if err == nil {
		err = io.ErrNoProgress
	}
	return err
}

// ReadPacket returns the next packet, reading as needed.
func (r *Reader) ReadPacket() (id int32, body []byte, err error) {
	for {
		id, body, total, err := ParsePacket(r.buf[r.off:], r.max)
		if err == nil {
			r.off += total
			return id, body, nil
		}
		if !errors.Is(err, ErrShort) {
			return 0, nil, err
		}
		if err := r.Fill(); err != nil {
			if errors.Is(err, io.EOF) && len(r.buf) > r.off {
				err = io.ErrUnexpectedEOF
			}
			return 0, nil, err
		}
	}
}

// QueryStatus performs a server list ping on rw (handshake with intent 1 and
// the given protocol, then Status Request) and returns the response JSON.
// The caller sets deadlines and sends any PROXY header first.
func QueryStatus(rw io.ReadWriter, protocol int32, host string, port uint16) (string, error) {
	req := AppendHandshake(nil, Handshake{Protocol: protocol, Host: host, Port: port, Intent: IntentStatus})
	req = append(req, StatusRequest...)
	if _, err := rw.Write(req); err != nil {
		return "", err
	}
	id, body, err := NewReader(rw, 1<<20).ReadPacket()
	if err != nil {
		return "", err
	}
	if id != 0x00 {
		return "", fmt.Errorf("%w: status response has id %#x", ErrMalformed, id)
	}
	return ParseStatusResponse(body)
}
