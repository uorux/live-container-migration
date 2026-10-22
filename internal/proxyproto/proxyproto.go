// Package proxyproto builds and parses HAProxy PROXY protocol v2 headers
// (TCP over IPv4/IPv6 only, no TLVs).
package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
)

// Signature is the 12-byte v2 signature.
var Signature = []byte("\r\n\r\n\x00\r\nQUIT\n")

const (
	cmdLocal = 0x20
	cmdProxy = 0x21
	famTCP4  = 0x11
	famTCP6  = 0x21
	famUnspc = 0x00
)

// Header returns a v2 header for a TCP connection from src to dst. If either
// address is invalid, it returns a LOCAL header (the server then uses the real
// socket addresses). Mixed families are encoded as IPv6 (v4-mapped).
func Header(src, dst netip.AddrPort) []byte {
	b := append([]byte{}, Signature...)
	if !src.IsValid() || !dst.IsValid() {
		return append(b, cmdLocal, famUnspc, 0, 0)
	}
	s, d := src.Addr().Unmap(), dst.Addr().Unmap()
	if s.Is4() && d.Is4() {
		b = append(b, cmdProxy, famTCP4, 0, 12)
		s4, d4 := s.As4(), d.As4()
		b = append(b, s4[:]...)
		b = append(b, d4[:]...)
	} else {
		b = append(b, cmdProxy, famTCP6, 0, 36)
		s16, d16 := s.As16(), d.As16()
		b = append(b, s16[:]...)
		b = append(b, d16[:]...)
	}
	b = binary.BigEndian.AppendUint16(b, src.Port())
	return binary.BigEndian.AppendUint16(b, dst.Port())
}

// ErrInvalid is returned for malformed headers.
var ErrInvalid = errors.New("invalid PROXY v2 header")

// Info is a parsed header. Local is true for the LOCAL command.
type Info struct {
	Local    bool
	Src, Dst netip.AddrPort
}

// Read consumes one v2 header from r.
func Read(r *bufio.Reader) (Info, error) {
	var info Info
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return info, err
	}
	if !bytes.Equal(hdr[:12], Signature) || hdr[12]>>4 != 2 {
		return info, ErrInvalid
	}
	n := binary.BigEndian.Uint16(hdr[14:16])
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return info, err
	}
	switch hdr[12] {
	case cmdLocal:
		info.Local = true
		return info, nil
	case cmdProxy:
	default:
		return info, ErrInvalid
	}
	switch hdr[13] {
	case famTCP4:
		if n < 12 {
			return info, ErrInvalid
		}
		info.Src = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[0:4])), binary.BigEndian.Uint16(body[8:10]))
		info.Dst = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[4:8])), binary.BigEndian.Uint16(body[10:12]))
	case famTCP6:
		if n < 36 {
			return info, ErrInvalid
		}
		info.Src = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[0:16])), binary.BigEndian.Uint16(body[32:34]))
		info.Dst = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[16:32])), binary.BigEndian.Uint16(body[34:36]))
	default:
		return info, ErrInvalid
	}
	return info, nil
}
