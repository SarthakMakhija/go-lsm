package kv

// NewStringKeyWithTimestamp creates a new instance of Key.
// It is only used for tests.
func NewStringKeyWithTimestamp(key string, timestamp uint64) Key {
	return Key{key: []byte(key), timestamp: timestamp}
}
