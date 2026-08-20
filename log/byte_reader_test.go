package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteReaderReadsUint16(t *testing.T) {
	buf := []byte{0x01, 0x00, 0x02, 0x00}
	reader := newByteReader(buf)

	assert.Equal(t, uint16(1), reader.uint16())
	assert.Equal(t, uint16(2), reader.uint16())
}

func TestByteReaderTakesNBytes(t *testing.T) {
	buf := []byte{'a', 'b', 'c', 'd', 'e'}
	reader := newByteReader(buf)

	assert.Equal(t, []byte{'a', 'b'}, reader.take(2))
	assert.Equal(t, []byte{'c', 'd', 'e'}, reader.take(3))
}

func TestByteReaderMixedUint16AndTake(t *testing.T) {
	buf := []byte{0x03, 0x00, 'x', 'y', 'z'}
	reader := newByteReader(buf)

	assert.Equal(t, uint16(3), reader.uint16())
	assert.Equal(t, []byte{'x', 'y', 'z'}, reader.take(3))
}

func TestByteReaderRemaining(t *testing.T) {
	buf := []byte{0x00, 0x00, 0x00, 0x00}
	reader := newByteReader(buf)

	assert.Equal(t, 4, reader.remaining())
	reader.uint16()
	assert.Equal(t, 2, reader.remaining())
	reader.take(2)
	assert.Equal(t, 0, reader.remaining())
}

func TestByteReaderRemainingOnEmptyBuffer(t *testing.T) {
	reader := newByteReader(nil)
	assert.Equal(t, 0, reader.remaining())
}
