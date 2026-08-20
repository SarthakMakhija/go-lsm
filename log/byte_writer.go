package log

import "encoding/binary"

// byteWriter is a sequential cursor over a []byte buffer.
// It is used to encode WAL records without manually computing byte offsets.
type byteWriter struct {
	buf []byte
	pos int
}

func newByteWriter(buf []byte) *byteWriter {
	return &byteWriter{buf: buf}
}

// uint16 writes v as a little-endian uint16 and advances the cursor.
func (writer *byteWriter) uint16(v uint16) {
	binary.LittleEndian.PutUint16(writer.buf[writer.pos:], v)
	writer.pos += 2
}

// bytes copies b into the buffer and advances the cursor.
func (writer *byteWriter) bytes(b []byte) {
	writer.pos += copy(writer.buf[writer.pos:], b)
}
