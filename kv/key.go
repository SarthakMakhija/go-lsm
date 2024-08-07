package kv

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

func NewKey(key []byte, timestamp uint64) Key {
	return Key{
		key:       key,
		timestamp: timestamp,
	}
}

func (key Key) IsLessThanOrEqualTo(other LessOrEqual) bool {
	otherKey := other.(Key)
	comparison := bytes.Compare(key.key, otherKey.key)
	if comparison > 0 {
		return false
	}
	if comparison < 0 {
		return true
	}
	//comparison == 0
	return key.timestamp <= otherKey.timestamp
}

func (key Key) IsEqualTo(other Key) bool {
	return bytes.Compare(key.key, other.key) == 0 && key.timestamp == other.timestamp
}

func (key Key) CompareKeysWithDescendingTimestamp(other Key) int {
	comparison := bytes.Compare(key.key, other.key)
	if comparison != 0 {
		return comparison
	}
	if key.timestamp == other.timestamp {
		return 0
	}
	if key.timestamp > other.timestamp {
		return -1
	}
	return 1
}

func CompareKeys(userKey, systemKey Key) int {
	return userKey.CompareKeysWithDescendingTimestamp(systemKey)
}

func (key Key) IsRawKeyEqualTo(other Key) bool {
	return bytes.Compare(key.key, other.key) == 0
}

func (key Key) IsRawKeyEmpty() bool {
	return key.RawSizeInBytes() == 0
}

func (key Key) EncodedBytes() []byte {
	if key.IsRawKeyEmpty() {
		return nil
	}
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
	if key.IsRawKeyEmpty() {
		return 0
	}
	return len(key.key) + TimestampSize
}

func (key Key) RawSizeInBytes() int {
	return len(key.RawBytes())
}

func (key Key) Timestamp() uint64 {
	return key.timestamp
}
