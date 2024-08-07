//go:build test

package kv

func NewStringKeyWithTimestamp(key string, timestamp uint64) Key {
	return Key{key: []byte(key), timestamp: timestamp}
}
