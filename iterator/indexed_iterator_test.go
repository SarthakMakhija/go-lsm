package iterator

import (
	"go-lsm/kv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThePriorityOfIndexedIteratorBasedOnKey(t *testing.T) {
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

func TestThePriorityOfIndexedIteratorBasedOnSameKeyWithDifferentIteratorIndex(t *testing.T) {
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

func TestThePriorityOfIndexedIteratorBasedOnSameKeyWithDifferentTimestamp(t *testing.T) {
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
