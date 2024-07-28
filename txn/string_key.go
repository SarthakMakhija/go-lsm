//go:build test

package txn

func NewStringKey(key string) Key {
	return Key{key: []byte(key), timestamp: 0}
}
