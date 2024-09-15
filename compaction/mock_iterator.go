package compaction

import "go-lsm/kv"

type mockIterator struct {
	keys         []kv.Key
	values       []kv.Value
	currentIndex int
}

func newMockIterator(keys []kv.Key, values []kv.Value) *mockIterator {
	return &mockIterator{
		keys:         keys,
		values:       values,
		currentIndex: 0,
	}
}

func (iterator *mockIterator) Key() kv.Key {
	return iterator.keys[iterator.currentIndex]
}

func (iterator *mockIterator) Value() kv.Value {
	return iterator.values[iterator.currentIndex]
}

func (iterator *mockIterator) Next() error {
	iterator.currentIndex++
	return nil
}

func (iterator *mockIterator) IsValid() bool {
	return iterator.currentIndex < len(iterator.keys)
}

func (iterator *mockIterator) Close() {
}
