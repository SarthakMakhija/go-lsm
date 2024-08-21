package kv

// Value is a tiny wrapper over raw []byte slice.
type Value struct {
	value []byte
}

var EmptyValue = Value{value: nil}

// NewValue creates a new instance of Value
func NewValue(value []byte) Value {
	return Value{value: value}
}

// IsEmpty returns true if the Value is empty.
func (value Value) IsEmpty() bool {
	return len(value.value) == 0
}

// String returns the string representation of Value.
func (value Value) String() string {
	return string(value.value)
}

// SizeInBytes returns the length of the raw byte slice.
func (value Value) SizeInBytes() int {
	return len(value.value)
}

// SizeAsUint32 returns the size as uint32.
func (value Value) SizeAsUint32() uint32 {
	return uint32(value.SizeInBytes())
}

// EncodeTo writes the raw byte slice to the provided buffer.
// It is mainly called from external.SkipList.
func (value *Value) EncodeTo(buffer []byte) uint32 {
	return uint32(copy(buffer, value.value))
}

// DecodeFrom sets the provided byte slice as its value.
// It is mainly called from external.SkipList.
func (value *Value) DecodeFrom(buffer []byte) {
	value.value = buffer
}

// Bytes returns the raw value.
func (value Value) Bytes() []byte {
	return value.value
}
