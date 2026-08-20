package log

import "encoding/binary"

// byteReader is a sequential cursor over a []byte buffer.
// It is used to decode WAL records without manually computing byte offsets.
type byteReader struct {
	buf []byte
	pos int
}

func newByteReader(buf []byte) *byteReader {
	return &byteReader{buf: buf}
}

// uint16 reads the next 2 bytes as a little-endian uint16 and advances the cursor.
func (reader *byteReader) uint16() uint16 {
	value := binary.LittleEndian.Uint16(reader.buf[reader.pos:])
	reader.pos += 2
	return value
}

// take returns the next n bytes and advances the cursor.
func (reader *byteReader) take(n int) []byte {
	value := reader.buf[reader.pos : reader.pos+n]
	reader.pos += n
	return value
}

// remaining returns the number of unread bytes left in the buffer.
func (reader *byteReader) remaining() int {
	return len(reader.buf) - reader.pos
}
