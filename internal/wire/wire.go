// Package wire implements the frame format of the lcm tunnel protocol
// (see docs/tunnel-protocol.md). It is pure encoding/decoding; no state.
package wire

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
)

// Version is the protocol version carried in CHALLENGE and HELLO.
const Version = 1

// Magic opens the CHALLENGE frame so that a misdirected connection fails fast.
var Magic = [4]byte{'L', 'C', 'M', 'T'}

const (
	// HeaderLen is the size of the fixed frame header: type(1) stream(4) length(4).
	HeaderLen = 9
	// MaxPayload bounds any frame payload (HELLO with many streams is the largest).
	MaxPayload = 1 << 20
	// MaxData is the largest DATA chunk a sender emits.
	MaxData = 16 << 10
	// SessionIDLen is the size of a session identifier.
	SessionIDLen = 16
	// NonceLen is the size of the CHALLENGE nonce.
	NonceLen = 32
	macLen   = sha256.Size
)

// Type is a frame type.
type Type uint8

const (
	TChallenge Type = 1
	THello     Type = 2
	THelloAck  Type = 3
	TOpen      Type = 4
	TData      Type = 5
	TAck       Type = 6
	TClose     Type = 7
	TReset     Type = 8
	TPing      Type = 9
	TPong      Type = 10
	TGoAway    Type = 11
)

func (t Type) String() string {
	switch t {
	case TChallenge:
		return "CHALLENGE"
	case THello:
		return "HELLO"
	case THelloAck:
		return "HELLO_ACK"
	case TOpen:
		return "OPEN"
	case TData:
		return "DATA"
	case TAck:
		return "ACK"
	case TClose:
		return "CLOSE"
	case TReset:
		return "RESET"
	case TPing:
		return "PING"
	case TPong:
		return "PONG"
	case TGoAway:
		return "GOAWAY"
	}
	return fmt.Sprintf("TYPE(%d)", uint8(t))
}

// Frame is one decoded frame. Payload aliases the reader's buffer only until
// the next ReadFrame call when read with a Reader; callers that keep it must copy.
type Frame struct {
	Type    Type
	Stream  uint32
	Payload []byte
}

// ErrProtocol wraps all decoding errors caused by a misbehaving peer.
var ErrProtocol = errors.New("tunnel protocol error")

func protoErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, a...))
}

// AppendFrame appends an encoded frame to dst.
func AppendFrame(dst []byte, t Type, stream uint32, payload []byte) []byte {
	dst = append(dst, byte(t))
	dst = binary.BigEndian.AppendUint32(dst, stream)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(payload)))
	return append(dst, payload...)
}

// Reader decodes frames from a byte stream.
type Reader struct {
	r   *bufio.Reader
	buf []byte
}

// NewReader returns a frame reader on r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64<<10)}
}

// ReadFrame reads the next frame. The returned payload is valid until the next call.
func (r *Reader) ReadFrame() (Frame, error) {
	var hdr [HeaderLen]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[5:9])
	if n > MaxPayload {
		return Frame{}, protoErr("frame length %d exceeds max", n)
	}
	if cap(r.buf) < int(n) {
		r.buf = make([]byte, n)
	}
	p := r.buf[:n]
	if _, err := io.ReadFull(r.r, p); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	return Frame{Type: Type(hdr[0]), Stream: binary.BigEndian.Uint32(hdr[1:5]), Payload: p}, nil
}

// ---- payload helpers ----

type dec struct {
	b   []byte
	err error
}

func (d *dec) need(n int) bool {
	if d.err != nil {
		return false
	}
	if len(d.b) < n {
		d.err = protoErr("short payload")
		return false
	}
	return true
}
func (d *dec) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}
func (d *dec) u16() uint16 {
	if !d.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(d.b)
	d.b = d.b[2:]
	return v
}
func (d *dec) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}
func (d *dec) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}
func (d *dec) bytes(n int) []byte {
	if !d.need(n) {
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}
func (d *dec) str8() string  { return string(d.bytes(int(d.u8()))) }
func (d *dec) str16() string { return string(d.bytes(int(d.u16()))) }
func (d *dec) done() error {
	if d.err == nil && len(d.b) != 0 {
		d.err = protoErr("trailing bytes in payload")
	}
	return d.err
}

func appendStr8(b []byte, s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	return append(append(b, byte(len(s))), s...)
}
func appendStr16(b []byte, s string) []byte {
	if len(s) > 1024 {
		s = s[:1024]
	}
	return append(binary.BigEndian.AppendUint16(b, uint16(len(s))), s...)
}

// ---- CHALLENGE ----

// Challenge is sent by the gateway as the first frame of every tunnel connection.
type Challenge struct {
	Version uint16
	Nonce   [NonceLen]byte
}

func (c *Challenge) Append(b []byte) []byte {
	b = append(b, Magic[:]...)
	b = binary.BigEndian.AppendUint16(b, c.Version)
	return append(b, c.Nonce[:]...)
}

func ParseChallenge(p []byte) (Challenge, error) {
	d := dec{b: p}
	var c Challenge
	if m := d.bytes(4); d.err == nil && [4]byte(m) != Magic {
		return c, protoErr("bad magic")
	}
	c.Version = d.u16()
	copy(c.Nonce[:], d.bytes(NonceLen))
	return c, d.done()
}

// ---- stream state entries used by HELLO / HELLO_ACK ----

// Entry flag bits.
const (
	// EntryFin: the sender of the entry has received the peer's CLOSE for this stream.
	EntryFin = 1 << 0
)

// StreamState is one stream's receive-side view, exchanged at (re)connect.
type StreamState struct {
	ID       uint32
	RxOffset uint64 // bytes of this stream received from the peer (and retained)
	Flags    uint8
}

func appendEntries(b []byte, es []StreamState) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(es)))
	for _, e := range es {
		b = binary.BigEndian.AppendUint32(b, e.ID)
		b = binary.BigEndian.AppendUint64(b, e.RxOffset)
		b = append(b, e.Flags)
	}
	return b
}

func (d *dec) entries() []StreamState {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if uint64(n)*13 > uint64(len(d.b)) {
		d.err = protoErr("entry count %d too large", n)
		return nil
	}
	es := make([]StreamState, n)
	for i := range es {
		es[i].ID = d.u32()
		es[i].RxOffset = d.u64()
		es[i].Flags = d.u8()
	}
	return es
}

// ---- HELLO ----

// Hello is the relay's first frame (answering CHALLENGE).
type Hello struct {
	Version uint16
	Session [SessionIDLen]byte
	Backend string
	// Window is the maximum number of unacknowledged bytes per stream the
	// gateway may send to this relay.
	Window uint32
	// MaxSeen is the highest stream ID for which this relay has processed an OPEN
	// in this session (0 if none).
	MaxSeen uint32
	Streams []StreamState
}

func helloMAC(token []byte, label string, nonce [NonceLen]byte, body []byte) []byte {
	m := hmac.New(sha256.New, token)
	m.Write([]byte(label))
	m.Write(nonce[:])
	m.Write(body)
	return m.Sum(nil)
}

// Marshal encodes the HELLO payload, authenticated with token over the nonce.
func (h *Hello) Marshal(token []byte, nonce [NonceLen]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, h.Version)
	b = append(b, h.Session[:]...)
	b = appendStr8(b, h.Backend)
	b = binary.BigEndian.AppendUint32(b, h.Window)
	b = binary.BigEndian.AppendUint32(b, h.MaxSeen)
	b = appendEntries(b, h.Streams)
	return append(b, helloMAC(token, "lcm-hello-v1", nonce, b)...)
}

// ErrAuth is returned when a MAC does not verify.
var ErrAuth = errors.New("tunnel authentication failed")

// ParseHello decodes and authenticates a HELLO payload.
func ParseHello(p []byte, token []byte, nonce [NonceLen]byte) (Hello, error) {
	var h Hello
	if len(p) < macLen {
		return h, protoErr("short hello")
	}
	body, mac := p[:len(p)-macLen], p[len(p)-macLen:]
	if !hmac.Equal(mac, helloMAC(token, "lcm-hello-v1", nonce, body)) {
		return h, ErrAuth
	}
	d := dec{b: body}
	h.Version = d.u16()
	copy(h.Session[:], d.bytes(SessionIDLen))
	h.Backend = d.str8()
	h.Window = d.u32()
	h.MaxSeen = d.u32()
	h.Streams = d.entries()
	return h, d.done()
}

// ---- HELLO_ACK ----

// HelloAck status values.
const (
	AckResumed  = 0 // gateway knew the session; resume streams per entries
	AckNew      = 1 // gateway started fresh state for this session id; relay drops all streams
	AckRejected = 2 // refused (bad version, unknown backend, ...); see Message
)

// HelloAck is the gateway's answer to HELLO.
type HelloAck struct {
	Status  uint8
	Window  uint32 // max unacked bytes per stream the relay may send
	Message string
	Streams []StreamState
}

// Marshal encodes the HELLO_ACK payload, authenticated over the nonce and session.
func (a *HelloAck) Marshal(token []byte, nonce [NonceLen]byte, session [SessionIDLen]byte) []byte {
	b := []byte{a.Status}
	b = binary.BigEndian.AppendUint32(b, a.Window)
	b = appendStr16(b, a.Message)
	b = appendEntries(b, a.Streams)
	full := append(append([]byte{}, session[:]...), b...)
	return append(b, helloMAC(token, "lcm-hello-ack-v1", nonce, full)...)
}

// ParseHelloAck decodes and authenticates a HELLO_ACK payload.
func ParseHelloAck(p []byte, token []byte, nonce [NonceLen]byte, session [SessionIDLen]byte) (HelloAck, error) {
	var a HelloAck
	if len(p) < macLen {
		return a, protoErr("short hello-ack")
	}
	body, mac := p[:len(p)-macLen], p[len(p)-macLen:]
	full := append(append([]byte{}, session[:]...), body...)
	if !hmac.Equal(mac, helloMAC(token, "lcm-hello-ack-v1", nonce, full)) {
		return a, ErrAuth
	}
	d := dec{b: body}
	a.Status = d.u8()
	a.Window = d.u32()
	a.Message = d.str16()
	a.Streams = d.entries()
	return a, d.done()
}

// ---- OPEN ----

// Open announces a new stream (gateway -> relay) with the player's addresses.
type Open struct {
	Src, Dst netip.AddrPort
}

func appendAddrPort(b []byte, ap netip.AddrPort) []byte {
	a := ap.Addr().Unmap()
	switch {
	case a.Is4():
		b = append(b, 4)
		v := a.As4()
		b = append(b, v[:]...)
	case a.Is6():
		b = append(b, 6)
		v := a.As16()
		b = append(b, v[:]...)
	default:
		return append(b, 0)
	}
	return binary.BigEndian.AppendUint16(b, ap.Port())
}

func (d *dec) addrPort() netip.AddrPort {
	switch d.u8() {
	case 0:
		return netip.AddrPort{}
	case 4:
		a := netip.AddrFrom4([4]byte(d.bytes(4)))
		return netip.AddrPortFrom(a, d.u16())
	case 6:
		a := netip.AddrFrom16([16]byte(d.bytes(16)))
		return netip.AddrPortFrom(a, d.u16())
	default:
		if d.err == nil {
			d.err = protoErr("bad address family")
		}
		return netip.AddrPort{}
	}
}

func (o *Open) Append(b []byte) []byte {
	b = appendAddrPort(b, o.Src)
	return appendAddrPort(b, o.Dst)
}

func ParseOpen(p []byte) (Open, error) {
	d := dec{b: p}
	var o Open
	o.Src = d.addrPort()
	o.Dst = d.addrPort()
	return o, d.done()
}

// ---- DATA ----

// AppendDataFrame appends a whole DATA frame (header included).
func AppendDataFrame(dst []byte, stream uint32, offset uint64, data []byte) []byte {
	dst = append(dst, byte(TData))
	dst = binary.BigEndian.AppendUint32(dst, stream)
	dst = binary.BigEndian.AppendUint32(dst, uint32(8+len(data)))
	dst = binary.BigEndian.AppendUint64(dst, offset)
	return append(dst, data...)
}

// ParseData splits a DATA payload into its offset and bytes (aliasing p).
func ParseData(p []byte) (offset uint64, data []byte, err error) {
	if len(p) < 8 {
		return 0, nil, protoErr("short data")
	}
	return binary.BigEndian.Uint64(p), p[8:], nil
}

// ---- ACK ----

// Ack flag bits.
const (
	// AckFin: the receiver has received and delivered the peer's CLOSE (half-close
	// applied to its local socket).
	AckFin = 1 << 0
)

func AckPayload(offset uint64, flags uint8) []byte {
	return append(binary.BigEndian.AppendUint64(nil, offset), flags)
}

func ParseAck(p []byte) (offset uint64, flags uint8, err error) {
	d := dec{b: p}
	offset = d.u64()
	flags = d.u8()
	return offset, flags, d.done()
}

// ---- CLOSE ----

func ClosePayload(finOffset uint64) []byte { return binary.BigEndian.AppendUint64(nil, finOffset) }

func ParseClose(p []byte) (uint64, error) {
	d := dec{b: p}
	v := d.u64()
	return v, d.done()
}

// ---- RESET ----

// Reset reasons.
const (
	ResetGeneric     = 0
	ResetDialFailed  = 1 // relay could not connect to the server
	ResetLocal       = 2 // local socket failed (RST / write error)
	ResetProtocol    = 3 // stream-level protocol violation
	ResetSessionGone = 4
)

func ResetPayload(code uint8, msg string) []byte { return appendStr16([]byte{code}, msg) }

func ParseReset(p []byte) (code uint8, msg string, err error) {
	d := dec{b: p}
	code = d.u8()
	msg = d.str16()
	return code, msg, d.done()
}

// ---- PING / PONG ----

func PingPayload(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func ParsePing(p []byte) (uint64, error) {
	d := dec{b: p}
	v := d.u64()
	return v, d.done()
}

// ---- GOAWAY ----

// GoAway flag bits.
const (
	// GoAwayFinal: the session is ending for good (server process exited); the
	// gateway should close all of the session's streams instead of holding them.
	GoAwayFinal = 1 << 0
	// GoAwayRebind (relay, as the first frame after a HELLO_ACK of status NEW):
	// the relay processed nothing on this connection and reconnects at once
	// with a new session id; the gateway offers the streams it opened on this
	// connection again, as never opened.
	GoAwayRebind = 1 << 1
)

func GoAwayPayload(flags uint8, msg string) []byte { return appendStr16([]byte{flags}, msg) }

func ParseGoAway(p []byte) (flags uint8, msg string, err error) {
	d := dec{b: p}
	flags = d.u8()
	msg = d.str16()
	return flags, msg, d.done()
}
