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

func (iterator *testIteratorNoEndKey) Close() {
}

func TestMergeIteratorWithASingleIterator(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringKeyWithTimestamp("storage", 14)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iterator})
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithASingleInvalidIterator(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringKeyWithTimestamp("storage", 5)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iterator.currentIndex = 2
	mergeIterator := NewMergeIterator([]Iterator{iterator})
	defer mergeIterator.Close()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 3), txn.NewStringKeyWithTimestamp("storage", 7)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("diskType", 4), txn.NewStringKeyWithTimestamp("distributed-db", 7)},
		[]txn.Value{txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 3), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("diskType", 4), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed-db", 7), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("storage", 7), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey1(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringKeyWithTimestamp("diskType", 7), txn.NewStringKeyWithTimestamp("distributed-db", 8)},
		[]txn.Value{txn.NewStringValue("raft"), txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringKeyWithTimestamp("storage", 8)},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne})
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 7), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 6), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("diskType", 7), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed-db", 8), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("storage", 8), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey2(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringKeyWithTimestamp("diskType", 5), txn.NewStringKeyWithTimestamp("distributed-db", 6)},
		[]txn.Value{txn.NewStringValue("paxos"), txn.NewStringValue("SSD"), txn.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]txn.Key{txn.NewStringKeyWithTimestamp("accurate", 2), txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringKeyWithTimestamp("storage", 6)},
		[]txn.Value{txn.NewStringValue("consistency"), txn.NewStringValue("raft"), txn.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo})
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("accurate", 2), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("consistency"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 4), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("diskType", 5), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed-db", 6), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("storage", 6), mergeIterator.Key())
	assert.Equal(t, txn.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}
