package mctest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf16"
)

// DecodeNBT decodes network NBT (tag type, then payload; no root name) into
// Go values: map[string]any for compounds, []any for lists, string, int8,
// int16, int32, int64, float32, float64 and []byte/[]int32/[]int64 arrays.
// It returns the value and the number of bytes used.
func DecodeNBT(b []byte) (any, int, error) {
	d := &nbtDecoder{b: b}
	t := d.u8()
	v := d.payload(t, 0)
	return v, d.off, d.err
}

type nbtDecoder struct {
	b   []byte
	off int
	err error
}

func (d *nbtDecoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || d.off+n > len(d.b) {
		d.err = errors.New("nbt: truncated")
		return nil
	}
	p := d.b[d.off : d.off+n]
	d.off += n
	return p
}

func (d *nbtDecoder) u8() byte {
	if p := d.take(1); p != nil {
		return p[0]
	}
	return 0
}

func (d *nbtDecoder) u16() uint16 {
	if p := d.take(2); p != nil {
		return binary.BigEndian.Uint16(p)
	}
	return 0
}

func (d *nbtDecoder) u32() uint32 {
	if p := d.take(4); p != nil {
		return binary.BigEndian.Uint32(p)
	}
	return 0
}

func (d *nbtDecoder) u64() uint64 {
	if p := d.take(8); p != nil {
		return binary.BigEndian.Uint64(p)
	}
	return 0
}

// str decodes Java's modified UTF-8.
func (d *nbtDecoder) str() string {
	p := d.take(int(d.u16()))
	var units []uint16
	for i := 0; i < len(p); {
		c := p[i]
		switch {
		case c < 0x80:
			units = append(units, uint16(c))
			i++
		case c&0xe0 == 0xc0 && i+1 < len(p):
			units = append(units, uint16(c&0x1f)<<6|uint16(p[i+1]&0x3f))
			i += 2
		case c&0xf0 == 0xe0 && i+2 < len(p):
			units = append(units, uint16(c&0x0f)<<12|uint16(p[i+1]&0x3f)<<6|uint16(p[i+2]&0x3f))
			i += 3
		default:
			d.err = fmt.Errorf("nbt: bad modified UTF-8 byte %#x", c)
			return ""
		}
	}
	return string(utf16.Decode(units))
}

func (d *nbtDecoder) payload(t byte, depth int) any {
	if depth > 64 {
		d.err = errors.New("nbt: too deep")
	}
	if d.err != nil {
		return nil
	}
	switch t {
	case 1:
		return int8(d.u8())
	case 2:
		return int16(d.u16())
	case 3:
		return int32(d.u32())
	case 4:
		return int64(d.u64())
	case 5:
		return float32frombits(d.u32())
	case 6:
		return float64frombits(d.u64())
	case 7:
		return append([]byte{}, d.take(int(int32(d.u32())))...)
	case 8:
		return d.str()
	case 9:
		et := d.u8()
		n := int(int32(d.u32()))
		if n < 0 || n > len(d.b) {
			d.err = errors.New("nbt: bad list length")
			return nil
		}
		l := make([]any, 0, n)
		for i := 0; i < n && d.err == nil; i++ {
			l = append(l, d.payload(et, depth+1))
		}
		return l
	case 10:
		m := map[string]any{}
		for d.err == nil {
			ft := d.u8()
			if ft == 0 {
				break
			}
			name := d.str()
			m[name] = d.payload(ft, depth+1)
		}
		return m
	case 11:
		n := int(int32(d.u32()))
		a := make([]int32, 0, max(n, 0))
		for i := 0; i < n && d.err == nil; i++ {
			a = append(a, int32(d.u32()))
		}
		return a
	case 12:
		n := int(int32(d.u32()))
		a := make([]int64, 0, max(n, 0))
		for i := 0; i < n && d.err == nil; i++ {
			a = append(a, int64(d.u64()))
		}
		return a
	}
	d.err = fmt.Errorf("nbt: unknown tag type %d", t)
	return nil
}

func float32frombits(u uint32) float32 { return math.Float32frombits(u) }
func float64frombits(u uint64) float64 { return math.Float64frombits(u) }
