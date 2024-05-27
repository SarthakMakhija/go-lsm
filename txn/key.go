package txn

import "bytes"

type Key struct {
	key []byte
}

var EmptyKey = Key{key: nil}

func NewKey(key []byte) Key {
	return Key{key: key}
}

func NewStringKey(key string) Key {
	return Key{key: []byte(key)}
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

func (key Key) String() string {
	return string(key.key)
}

func (key Key) Size() int {
	return len(key.key)
}

func (key Key) IsEmpty() bool {
	return key.Size() == 0
}

func (key Key) Bytes() []byte {
	return key.key
}
