package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestAnIndexedIteratorBasedOnKey(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("distributed", 2)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestAnIndexedIteratorBasedOnSameKeyWithDifferentIteratorIndex(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestAnIndexedIteratorBasedOnSameKeyWithDifferentTimestamp(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5)},
		[]kv.Value{kv.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 6)},
		[]kv.Value{kv.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOther.IsPrioritizedOver(indexedIteratorOne))
}
