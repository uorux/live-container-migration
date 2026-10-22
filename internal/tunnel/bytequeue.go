package tunnel

// byteQueue is a FIFO of bytes: append at the back, discard from the front,
// read at arbitrary positions. It compacts lazily so memory stays bounded by
// roughly twice the live length.
type byteQueue struct {
	buf []byte
	off int
}

func (q *byteQueue) Len() int { return len(q.buf) - q.off }

func (q *byteQueue) Append(p []byte) {
	if q.off > 0 && q.off >= len(q.buf)/2 && len(q.buf)+len(p) > cap(q.buf) {
		n := copy(q.buf, q.buf[q.off:])
		q.buf = q.buf[:n]
		q.off = 0
	}
	q.buf = append(q.buf, p...)
}

// Discard drops n bytes from the front. n must be <= Len().
func (q *byteQueue) Discard(n int) {
	if n < 0 || n > q.Len() {
		panic("byteQueue: discard out of range")
	}
	q.off += n
	if q.off == len(q.buf) {
		q.buf = q.buf[:0]
		q.off = 0
		if cap(q.buf) > 256<<10 {
			q.buf = nil // release a big idle buffer
		}
	}
}

// CopyAt copies bytes starting at position pos (relative to the front) into
// dst and returns the count.
func (q *byteQueue) CopyAt(dst []byte, pos int) int {
	if pos < 0 || pos > q.Len() {
		panic("byteQueue: position out of range")
	}
	return copy(dst, q.buf[q.off+pos:])
}

// AppendAt appends up to n bytes starting at pos to dst.
func (q *byteQueue) AppendAt(dst []byte, pos, n int) []byte {
	start := q.off + pos
	return append(dst, q.buf[start:start+n]...)
}
