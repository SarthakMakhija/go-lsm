package txn

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const TimestampSize = int(unsafe.Sizeof(uint64(0)))

type Key struct {
	key       []byte
	timestamp uint64
}

var EmptyKey = Key{key: nil}

func DecodeFrom(buffer []byte) Key {
	if len(buffer) < TimestampSize {
		panic("buffer too small to decode the key from")
	}
	length := len(buffer)
	return Key{
		key:       buffer[:length-TimestampSize],
		timestamp: binary.LittleEndian.Uint64(buffer[length-TimestampSize:]),
	}
}

func (key Key) IsLessThanOrEqualTo(other Key) bool {
	return bytes.Compare(key.key, other.key) <= 0
}

func (key Key) IsEqualTo(other Key) bool {
	return bytes.Compare(key.key, other.key) == 0
}

func (key Key) Compare(other Key) int {
	return bytes.Compare(key.key, other.key)
}

func (key Key) IsRawKeyEmpty() bool {
	return key.RawSizeInBytes() == 0
}

func (key Key) EncodedBytes() []byte {
	buffer := make([]byte, key.EncodedSizeInBytes())

	numberOfBytesWritten := copy(buffer, key.key)
	binary.LittleEndian.PutUint64(buffer[numberOfBytesWritten:], key.timestamp)

	return buffer
}

func (key Key) RawBytes() []byte {
	return key.key
}

func (key Key) RawString() string {
	return string(key.key)
}

func (key Key) EncodedSizeInBytes() int {
	return len(key.key) + TimestampSize
}

func (key Key) RawSizeInBytes() int {
	return len(key.RawBytes())
}
