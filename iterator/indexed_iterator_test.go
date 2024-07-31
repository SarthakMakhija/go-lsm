package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAnIndexedIteratorBasedOnKey(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10)},
		[]txn.Value{txn.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("distributed", 2)},
		[]txn.Value{txn.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestAnIndexedIteratorBasedOnSameKeyWithDifferentIteratorIndex(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 5)},
		[]txn.Value{txn.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 5)},
		[]txn.Value{txn.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestAnIndexedIteratorBasedOnSameKeyWithDifferentTimestamp(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 5)},
		[]txn.Value{txn.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 6)},
		[]txn.Value{txn.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOther.IsPrioritizedOver(indexedIteratorOne))
}
