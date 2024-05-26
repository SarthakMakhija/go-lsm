package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAnIndexedIteratorBasedOnKey(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus")},
		[]txn.Value{txn.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("distributed")},
		[]txn.Value{txn.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}

func TestAnIndexedIteratorBasedOnSameKeyWithDifferentIteratorIndex(t *testing.T) {
	indexedIteratorOne := NewIndexedIterator(0, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus")},
		[]txn.Value{txn.NewStringValue("raft")},
	))
	indexedIteratorOther := NewIndexedIterator(1, newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus")},
		[]txn.Value{txn.NewStringValue("db")},
	))

	assert.True(t, indexedIteratorOne.IsPrioritizedOver(indexedIteratorOther))
}
