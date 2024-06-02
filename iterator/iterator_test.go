package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestInclusiveBoundedIteratorWithTwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("diskType"), txn.NewStringKey("distributed-db")},
		[]txn.Value{txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKey("diskType"))

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("diskType"), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndADeletedKeyWithEmptyValue(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("diskType"), txn.NewStringKey("distributed-db")},
		[]txn.Value{txn.NewStringValue(""), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKey("diskType"))

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}
