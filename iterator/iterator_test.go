package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestInclusiveBoundedIteratorWithTwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 20)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("diskType", 30), txn.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]txn.Value{txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKeyWithTimestamp("diskType", 40))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("diskType", 30), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndADeletedKeyWithEmptyValue(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 20)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("diskType", 30), txn.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]txn.Value{txn.NewStringValue(""), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKeyWithTimestamp("diskType", 30))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAnInclusiveKeyWithSmallerTimestamp(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 20)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("diskType", 30), txn.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]txn.Value{txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKeyWithTimestamp("diskType", 20))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAndAKeyWithMultipleTimestamps(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 20)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKeyWithTimestamp("diskType", 20))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 20), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAndAKeyWithMultipleTimestampsWithOneTimestampGreaterThanRequested(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 20)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, txn.NewStringKeyWithTimestamp("storage", 11))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}
