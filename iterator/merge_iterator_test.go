package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

type testIteratorNoEndKey struct {
	keys         []txn.Key
	values       []txn.Value
	currentIndex int
}

func newTestIteratorNoEndKey(keys []txn.Key, values []txn.Value) *testIteratorNoEndKey {
	return &testIteratorNoEndKey{
		keys:         keys,
		values:       values,
		currentIndex: 0,
	}
}

func (iterator *testIteratorNoEndKey) Key() txn.Key {
	return iterator.keys[iterator.currentIndex]
}

func (iterator *testIteratorNoEndKey) Value() txn.Value {
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
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iterator})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("diskType"), txn.NewStringKey("distributed-db")},
		[]txn.Value{txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey1(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("diskType"), txn.NewStringKey("distributed-db")},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey2(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("consensus"), txn.NewStringKey("diskType"), txn.NewStringKey("distributed-db")},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKey("accurate"), txn.NewStringKey("consensus"), txn.NewStringKey("storage")},
		[]txn.Value{txn.NewStringValue("consistency"), txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("accurate"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("consistency"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("diskType"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed-db"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKey("storage"), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}
