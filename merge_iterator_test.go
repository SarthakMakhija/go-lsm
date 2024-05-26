package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testIteratorNoEndKey struct {
	keys         []Key
	values       []Value
	currentIndex int
}

func newTestIteratorNoEndKey(keys []Key, values []Value) *testIteratorNoEndKey {
	return &testIteratorNoEndKey{
		keys:         keys,
		values:       values,
		currentIndex: 0,
	}
}

func (iterator *testIteratorNoEndKey) Key() Key {
	return iterator.keys[iterator.currentIndex]
}

func (iterator *testIteratorNoEndKey) Value() Value {
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
		[]Key{NewStringKey("consensus"), NewStringKey("storage")},
		[]Value{NewStringValue("raft"), NewStringValue("NVMe")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iterator})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]Key{NewStringKey("consensus"), NewStringKey("storage")},
		[]Value{NewStringValue("raft"), NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]Key{NewStringKey("diskType"), NewStringKey("distributed-db")},
		[]Value{NewStringValue("SSD"), NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey1(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]Key{NewStringKey("consensus"), NewStringKey("diskType"), NewStringKey("distributed-db")},
		[]Value{NewStringValue("paxos"), NewStringValue("SSD"), NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]Key{NewStringKey("consensus"), NewStringKey("storage")},
		[]Value{NewStringValue("raft"), NewStringValue("NVMe")},
	)
	//iterator with the higher index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey2(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]Key{NewStringKey("consensus"), NewStringKey("diskType"), NewStringKey("distributed-db")},
		[]Value{NewStringValue("paxos"), NewStringValue("SSD"), NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]Key{NewStringKey("accurate"), NewStringKey("consensus"), NewStringKey("storage")},
		[]Value{NewStringValue("consistency"), NewStringValue("raft"), NewStringValue("NVMe")},
	)
	//iterator with the higher index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("accurate"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("consistency"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}
