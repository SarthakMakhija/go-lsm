package iterator

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

type testIteratorNoEndKey struct {
	keys         []kv.Key
	values       []kv.Value
	currentIndex int
}

func newTestIteratorNoEndKey(keys []kv.Key, values []kv.Value) *testIteratorNoEndKey {
	return &testIteratorNoEndKey{
		keys:         keys,
		values:       values,
		currentIndex: 0,
	}
}

func (iterator *testIteratorNoEndKey) Key() kv.Key {
	return iterator.keys[iterator.currentIndex]
}

func (iterator *testIteratorNoEndKey) Value() kv.Value {
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

func TestMergeIteratorWithAnOnCloseCallback(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 14)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	nothingCounter := 20
	counterDecrementingCallback := func() {
		nothingCounter -= 1
	}
	mergeIterator := NewMergeIterator([]Iterator{iterator}, counterDecrementingCallback)
	mergeIterator.Close()

	assert.Equal(t, 19, nothingCounter)
}

func TestMergeIteratorWithASingleIterator(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringKeyWithTimestamp("storage", 14)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iterator}, NoOperationOnCloseCallback)
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()
	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithASingleInvalidIterator(t *testing.T) {
	iterator := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringKeyWithTimestamp("storage", 5)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iterator.currentIndex = 2
	mergeIterator := NewMergeIterator([]Iterator{iterator}, NoOperationOnCloseCallback)
	defer mergeIterator.Close()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIterators(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 3), kv.NewStringKeyWithTimestamp("storage", 7)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("diskType", 4), kv.NewStringKeyWithTimestamp("distributed-db", 7)},
		[]kv.Value{kv.NewStringValue("SSD"), kv.NewStringValue("etcd")},
	)
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo}, NoOperationOnCloseCallback)
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 3), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("diskType", 4), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed-db", 7), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 7), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey1(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringKeyWithTimestamp("diskType", 7), kv.NewStringKeyWithTimestamp("distributed-db", 8)},
		[]kv.Value{kv.NewStringValue("raft"), kv.NewStringValue("SSD"), kv.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringKeyWithTimestamp("storage", 8)},
		[]kv.Value{kv.NewStringValue("paxos"), kv.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorTwo, iteratorOne}, NoOperationOnCloseCallback)
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 7), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 6), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("diskType", 7), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed-db", 8), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 8), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}

func TestMergeIteratorWithATwoIteratorsHavingSameKey2(t *testing.T) {
	iteratorOne := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringKeyWithTimestamp("diskType", 5), kv.NewStringKeyWithTimestamp("distributed-db", 6)},
		[]kv.Value{kv.NewStringValue("paxos"), kv.NewStringValue("SSD"), kv.NewStringValue("etcd")},
	)
	iteratorTwo := newTestIteratorNoEndKey(
		[]kv.Key{kv.NewStringKeyWithTimestamp("accurate", 2), kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringKeyWithTimestamp("storage", 6)},
		[]kv.Value{kv.NewStringValue("consistency"), kv.NewStringValue("raft"), kv.NewStringValue("NVMe")},
	)
	//iterator with the lower index has higher priority
	mergeIterator := NewMergeIterator([]Iterator{iteratorOne, iteratorTwo}, NoOperationOnCloseCallback)
	defer mergeIterator.Close()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("accurate", 2), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("consistency"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 4), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("diskType", 5), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed-db", 6), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("etcd"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.True(t, mergeIterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 6), mergeIterator.Key())
	assert.Equal(t, kv.NewStringValue("NVMe"), mergeIterator.Value())

	_ = mergeIterator.Next()

	assert.False(t, mergeIterator.IsValid())
}
