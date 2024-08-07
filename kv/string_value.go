//go:build test

package kv

func NewStringValue(value string) Value {
	return Value{value: []byte(value)}
}
