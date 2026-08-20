package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteWriterWritesUint16(t *testing.T) {
	buf := make([]byte, 4)
	writer := newByteWriter(buf)

	writer.uint16(1)
	writer.uint16(2)

	assert.Equal(t, []byte{0x01, 0x00, 0x02, 0x00}, buf)
}

func TestByteWriterWritesBytes(t *testing.T) {
	buf := make([]byte, 5)
	writer := newByteWriter(buf)

	writer.bytes([]byte{'a', 'b'})
	writer.bytes([]byte{'c', 'd', 'e'})

	assert.Equal(t, []byte{'a', 'b', 'c', 'd', 'e'}, buf)
}

func TestByteWriterMixedUint16AndBytes(t *testing.T) {
	buf := make([]byte, 5)
	writer := newByteWriter(buf)

	writer.uint16(3)
	writer.bytes([]byte{'x', 'y', 'z'})

	assert.Equal(t, []byte{0x03, 0x00, 'x', 'y', 'z'}, buf)
}
