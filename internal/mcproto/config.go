package mcproto

import (
	"encoding/binary"
	"fmt"
)

// The login and configuration packets below are what lcm-gateway's server
// menu speaks to a client that it logs in itself (offline mode, no
// encryption or compression) and keeps in the configuration phase, showing a
// dialog, until it transfers the player to the chosen server. Dialogs exist
// since 1.21.6 (protocol 771). IDs were taken from each release's data
// generator report (generated/reports/packets.json).

// Login-phase packet IDs, the same from 1.20.5 to at least 26.3.
const (
	LoginSuccessID        = 0x02 // clientbound login_finished
	LoginCookieRequestID  = 0x05 // clientbound cookie_request
	LoginAcknowledgedID   = 0x03 // serverbound login_acknowledged
	LoginCookieResponseID = 0x04 // serverbound cookie_response
)

// Configuration-phase packet IDs that did not move between 1.21.6 and 26.3.
const (
	ConfigDisconnectID  = 0x02 // clientbound disconnect
	ConfigKeepAliveID   = 0x04 // clientbound and serverbound keep_alive
	ConfigCustomClickID = 0x08 // serverbound custom_click_action
)

// MenuProtocol describes the version-dependent parts of the packets above.
type MenuProtocol struct {
	StoreCookieID, TransferID, ShowDialogID int32
	// LoginSessionID: Login Success ends with a session UUID (26.3+).
	LoginSessionID bool
}

// Oldest and newest protocols with a known MenuProtocol.
const (
	MenuMinProtocol = 771 // 1.21.6: dialogs
	MenuMaxProtocol = 777 // 26.3
)

// MenuProtocolFor returns the packet layout for a client protocol version.
func MenuProtocolFor(protocol int32) (MenuProtocol, bool) {
	switch {
	case protocol >= MenuMinProtocol && protocol <= 776: // 1.21.6 - 26.2
		return MenuProtocol{StoreCookieID: 0x0a, TransferID: 0x0b, ShowDialogID: 0x12}, true
	case protocol == 777: // 26.3 added post_effects before store_cookie
		return MenuProtocol{StoreCookieID: 0x0b, TransferID: 0x0c, ShowDialogID: 0x13, LoginSessionID: true}, true
	}
	return MenuProtocol{}, false
}

// LoginStart is the serverbound Login Start packet; since 1.20.2 it is the
// player name followed by the profile UUID.
type LoginStart struct {
	Name string
	UUID [16]byte
}

// ParseLoginStart parses a 1.20.2+ Login Start body.
func ParseLoginStart(body []byte) (LoginStart, error) {
	f := field{b: body}
	ls := LoginStart{Name: f.str(16 * 4)}
	if f.err == nil && len(f.b) != 16 {
		f.err = fmt.Errorf("%w: Login Start has %d bytes after the name, want a UUID", ErrMalformed, len(f.b))
	}
	if f.err != nil {
		return LoginStart{}, f.err
	}
	copy(ls.UUID[:], f.b)
	return ls, nil
}

// LoginSuccess returns the framed Login Success for an offline-mode login:
// the profile (UUID, name, no properties), plus a session UUID where the
// protocol has one.
func (p MenuProtocol) LoginSuccess(uuid [16]byte, name string) []byte {
	body := append([]byte(nil), uuid[:]...)
	body = AppendString(body, name)
	body = AppendVarInt(body, 0)
	if p.LoginSessionID {
		var sid [16]byte
		copy(sid[:], uuid[:])
		sid[0] ^= 0x5a // any UUID will do; vanilla uses a random one
		body = append(body, sid[:]...)
	}
	return AppendPacket(nil, LoginSuccessID, body)
}

// CookieRequest returns the framed login-phase Cookie Request.
func CookieRequest(key string) []byte {
	return AppendPacket(nil, LoginCookieRequestID, AppendString(nil, key))
}

// ParseCookieResponse parses a Cookie Response body: the key and the
// payload, nil if the client has no such cookie.
func ParseCookieResponse(body []byte) (key string, payload []byte, err error) {
	f := field{b: body}
	key = f.str(32767)
	if f.err != nil {
		return "", nil, f.err
	}
	if len(f.b) < 1 {
		return "", nil, fmt.Errorf("%w: short Cookie Response", ErrMalformed)
	}
	has := f.b[0]
	f.b = f.b[1:]
	if has == 0 {
		return key, nil, nil
	}
	n := f.varint()
	if f.err != nil || n < 0 || n > 5120 || int(n) > len(f.b) {
		return "", nil, fmt.Errorf("%w: cookie payload", ErrMalformed)
	}
	return key, append([]byte{}, f.b[:n]...), nil
}

// StoreCookie returns the framed configuration-phase Store Cookie. The
// client keeps cookies across a transfer, and hands them out on request.
func (p MenuProtocol) StoreCookie(key string, payload []byte) []byte {
	body := AppendString(nil, key)
	body = AppendVarInt(body, int32(len(payload)))
	return AppendPacket(nil, p.StoreCookieID, append(body, payload...))
}

// Transfer returns the framed configuration-phase Transfer: the client
// disconnects and connects to host:port with handshake intent 3.
func (p MenuProtocol) Transfer(host string, port int) []byte {
	return AppendPacket(nil, p.TransferID, AppendVarInt(AppendString(nil, host), int32(port)))
}

// ShowDialog returns the framed configuration-phase Show Dialog: the dialog
// definition inline, as NBT (the play-phase packet differs).
func (p MenuProtocol) ShowDialog(dialog Compound) []byte {
	return AppendPacket(nil, p.ShowDialogID, AppendNBT(nil, dialog))
}

// ConfigKeepAlive returns the framed clientbound configuration Keep Alive.
func ConfigKeepAlive(id int64) []byte {
	return AppendPacket(nil, ConfigKeepAliveID, binary.BigEndian.AppendUint64(nil, uint64(id)))
}

// ConfigDisconnect returns the framed configuration-phase Disconnect.
func ConfigDisconnect(reason NBT) []byte {
	return AppendPacket(nil, ConfigDisconnectID, AppendNBT(nil, reason))
}

// ParseCustomClick returns the identifier of a Custom Click Action (sent
// when the player presses a dialog button with a "minecraft:custom" action).
// The payload after it is ignored: its encoding changed between versions.
func ParseCustomClick(body []byte) (string, error) {
	f := field{b: body}
	id := f.str(32767)
	return id, f.err
}
