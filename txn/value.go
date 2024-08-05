package txn

type Value struct {
	value []byte
}

var EmptyValue = Value{value: nil}

func NewValue(value []byte) Value {
	return Value{value: value}
}

func (value Value) IsEmpty() bool {
	return len(value.value) == 0
}

func (value Value) String() string {
	return string(value.value)
}

func (value Value) SizeInBytes() int {
	return len(value.value)
}

func (value Value) SizeAsUint32() uint32 {
	return uint32(value.SizeInBytes())
}

func (value *Value) EncodeTo(buffer []byte) uint32 {
	return uint32(copy(buffer, value.value))
}

func (value *Value) DecodeFrom(buffer []byte) {
	value.value = buffer
}

func (value Value) Bytes() []byte {
	return value.value
}
