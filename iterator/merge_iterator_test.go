package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"testing"
)

type testIteratorNoEndKey struct {
	keys         []go_lsm.Key
	values       []go_lsm.Value
	currentIndex int
}

func newTestIteratorNoEndKey(keys []go_lsm.Key, values []go_lsm.Value) *testIteratorNoEndKey {
	return &testIteratorNoEndKey{
		keys:         keys,
		values:       values,
		currentIndex: 0,
	}
}

func (iterator *testIteratorNoEndKey) Key() go_lsm.Key {
	return iterator.keys[iterator.currentIndex]
}

func (iterator *testIteratorNoEndKey) Value() go_lsm.Value {
	return iterator.values[iterator.currentIndex]
}

func (iterator *testIteratorNoEndKey) Next() error {
	iterator.currentIndex++
	return nil
}

func (iterator *testIteratorNoEndKey) IsValid() bool {
	return iterator.currentIndex < len(iterator.keys)
}

func TestMergeIteratorWithASingleIterator(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("storage")},
		[]go_lsm.Value{go_lsm.NewStringValue("raft"), go_lsm.NewStringValue("NVMe")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iterator})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("storage")},
		[]go_lsm.Value{go_lsm.NewStringValue("raft"), go_lsm.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("diskType"), go_lsm.NewStringKey("distributed-db")},
		[]go_lsm.Value{go_lsm.NewStringValue("SSD"), go_lsm.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey1(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("diskType"), go_lsm.NewStringKey("distributed-db")},
		[]go_lsm.Value{go_lsm.NewStringValue("paxos"), go_lsm.NewStringValue("SSD"), go_lsm.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("storage")},
		[]go_lsm.Value{go_lsm.NewStringValue("raft"), go_lsm.NewStringValue("NVMe")},
	)
	//iterator with the higher index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey2(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("diskType"), go_lsm.NewStringKey("distributed-db")},
		[]go_lsm.Value{go_lsm.NewStringValue("paxos"), go_lsm.NewStringValue("SSD"), go_lsm.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]go_lsm.Key{go_lsm.NewStringKey("accurate"), go_lsm.NewStringKey("consensus"), go_lsm.NewStringKey("storage")},
		[]go_lsm.Value{go_lsm.NewStringValue("consistency"), go_lsm.NewStringValue("raft"), go_lsm.NewStringValue("NVMe")},
	)
	//iterator with the higher index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("accurate"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("consistency"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, go_lsm.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, go_lsm.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}
