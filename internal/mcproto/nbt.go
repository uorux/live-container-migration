package mcproto

import (
	"encoding/binary"
	"unicode/utf16"
)

// NBT is a value in Minecraft's Named Binary Tag format, as far as the
// gateway's server menu needs it: compounds (with ordered keys), lists,
// strings, bytes and ints. Text components and dialogs travel as NBT since
// 1.20.3 / 1.21.6.
type NBT interface {
	tagType() byte
	appendPayload(b []byte) []byte
}

// NBT tag types.
const (
	tagEnd      = 0
	tagByte     = 1
	tagInt      = 3
	tagString   = 8
	tagList     = 9
	tagCompound = 10
)

// Byte is TAG_Byte (also how booleans are stored).
type Byte int8

// Int is TAG_Int.
type Int int32

// String is TAG_String.
type String string

// List is TAG_List; every element must have the same tag type.
type List []NBT

// Compound is TAG_Compound; keys are written in order.
type Compound []Field

// Field is one named entry of a Compound.
type Field struct {
	Name  string
	Value NBT
}

// F returns a Field (for Compound literals outside this package).
func F(name string, v NBT) Field { return Field{name, v} }

// Bool returns Byte 1 or 0.
func Bool(v bool) Byte {
	if v {
		return 1
	}
	return 0
}

func (Byte) tagType() byte                     { return tagByte }
func (v Byte) appendPayload(b []byte) []byte   { return append(b, byte(v)) }
func (Int) tagType() byte                      { return tagInt }
func (v Int) appendPayload(b []byte) []byte    { return binary.BigEndian.AppendUint32(b, uint32(v)) }
func (String) tagType() byte                   { return tagString }
func (v String) appendPayload(b []byte) []byte { return appendMUTF8(b, string(v)) }
func (List) tagType() byte                     { return tagList }
func (Compound) tagType() byte                 { return tagCompound }

func (l List) appendPayload(b []byte) []byte {
	var t byte = tagEnd
	if len(l) > 0 {
		t = l[0].tagType()
	}
	b = append(b, t)
	b = binary.BigEndian.AppendUint32(b, uint32(len(l)))
	for _, v := range l {
		b = v.appendPayload(b)
	}
	return b
}

func (c Compound) appendPayload(b []byte) []byte {
	for _, f := range c {
		b = append(b, f.Value.tagType())
		b = appendMUTF8(b, f.Name)
		b = f.Value.appendPayload(b)
	}
	return append(b, tagEnd)
}

// AppendNBT appends v in the network form used since 1.20.2: the tag type
// and payload, with no root name.
func AppendNBT(b []byte, v NBT) []byte {
	return v.appendPayload(append(b, v.tagType()))
}

// appendMUTF8 appends s as Java's DataOutput.writeUTF does: a big-endian
// uint16 byte length, then "modified UTF-8" (NUL as C0 80, characters
// outside the BMP as two 3-byte surrogates). Longer strings are cut short.
func appendMUTF8(b []byte, s string) []byte {
	var enc []byte
	for _, r := range s {
		var units []uint16
		if r >= 0x10000 {
			r1, r2 := utf16.EncodeRune(r)
			units = []uint16{uint16(r1), uint16(r2)}
		} else {
			units = []uint16{uint16(r)}
		}
		for _, u := range units {
			var ch []byte
			switch {
			case u != 0 && u < 0x80:
				ch = []byte{byte(u)}
			case u < 0x800:
				ch = []byte{0xc0 | byte(u>>6), 0x80 | byte(u&0x3f)}
			default:
				ch = []byte{0xe0 | byte(u>>12), 0x80 | byte(u>>6&0x3f), 0x80 | byte(u&0x3f)}
			}
			if len(enc)+len(ch) > 0xffff {
				return append(binary.BigEndian.AppendUint16(b, uint16(len(enc))), enc...)
			}
			enc = append(enc, ch...)
		}
	}
	return append(binary.BigEndian.AppendUint16(b, uint16(len(enc))), enc...)
}

// Text returns a plain text component.
func Text(s string) NBT { return String(s) }

// Styled returns a text component with a color ("green", "#ff8800"; empty
// for none) and optionally bold.
func Styled(s, color string, bold bool) Compound {
	c := Compound{{"text", String(s)}}
	if color != "" {
		c = append(c, Field{"color", String(color)})
	}
	if bold {
		c = append(c, Field{"bold", Bool(true)})
	}
	return c
}

// Join returns a text component made of parts.
func Join(parts ...Compound) Compound {
	l := make(List, len(parts))
	for i, p := range parts {
		l[i] = p
	}
	return Compound{{"text", String("")}, {"extra", l}}
}
