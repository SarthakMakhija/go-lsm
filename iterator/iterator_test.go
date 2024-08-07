package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestInclusiveBoundedIteratorWithTwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 20)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("diskType", 30), kv.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]kv.Value{kv.NewStringValue("SSD"), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, kv.NewStringKeyWithTimestamp("diskType", 40))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("diskType", 30), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndADeletedKeyWithEmptyValue(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 20)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("diskType", 30), kv.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]kv.Value{kv.NewStringValue(""), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, kv.NewStringKeyWithTimestamp("diskType", 30))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAnInclusiveKeyWithSmallerTimestamp(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 20)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("diskType", 30), kv.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]kv.Value{kv.NewStringValue("SSD"), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, kv.NewStringKeyWithTimestamp("diskType", 20))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAndAKeyWithMultipleTimestamps(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 20)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]kv.Value{kv.NewStringValue("paxos"), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, kv.NewStringKeyWithTimestamp("diskType", 20))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 20), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}

func TestInclusiveBoundedIteratorWithTwoIteratorsAndAndAKeyWithMultipleTimestampsWithOneTimestampGreaterThanRequested(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 20)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringKeyWithTimestamp("distributed-db", 40)},
		[]kv.Value{kv.NewStringValue("paxos"), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	inclusiveBoundedIterator := NewInclusiveBoundedIterator(mergeIterator, kv.NewStringKeyWithTimestamp("storage", 11))
	defer inclusiveBoundedIterator.Close()

	assert.True(t, inclusiveBoundedIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), inclusiveBoundedIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), inclusiveBoundedIterator.Value())

	_ = inclusiveBoundedIterator.Next()
	assert.False(t, inclusiveBoundedIterator.IsValid())
}
