//go:build test

package txn

func NewStringKey(key string) Key {
	return NewStringKeyWithTimestamp(key, 0)
}

func NewStringKeyWithTimestamp(key string, timestamp uint64) Key {
	return Key{key: []byte(key), timestamp: timestamp}
}
