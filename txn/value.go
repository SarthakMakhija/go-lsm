package txn

type Value struct {
	value []byte
}

var EmptyValue = Value{value: nil}

func NewValue(value []byte) Value {
	return Value{value: value}
}

func NewStringValue(value string) Value {
	return Value{value: []byte(value)}
}

func (value Value) IsEmpty() bool {
	return len(value.value) == 0
}

func (value Value) String() string {
	return string(value.value)
}

func (value Value) Size() int {
	return len(value.value)
}
