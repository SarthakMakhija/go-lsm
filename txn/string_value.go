//go:build test

package txn

func NewStringValue(value string) Value {
	return Value{value: []byte(value)}
}
