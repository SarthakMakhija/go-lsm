package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestPendingWritesIteratorWithAnEmptyBatch(t *testing.T) {
	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("accurate"),
		kv.RawKey("etcd"),
	)
	iterator := NewPendingWritesIterator(kv.NewBatch(), 2, keyRange)
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorWithABatchContainingOneKeyValuePair(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("accurate"),
		kv.RawKey("etcd"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorWithABatchContainingOneDeletedKeyValuePair(t *testing.T) {
	batch := kv.NewBatch()
	batch.Delete([]byte("consensus"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("accurate"),
		kv.RawKey("etcd"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewValue(nil), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorWithABatchContainingFewPairs(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("accurate"),
		kv.RawKey("storage"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("bolt", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToTheStartKeyOfTheRange(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("consensus"),
		kv.RawKey("storage"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAMatchingKeyWithBoundCheck(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("consensus"),
		kv.RawKey("distributed"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheStartOfTheRange1(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("quantum"),
		kv.RawKey("storage"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheStartOfTheRange2(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("cart"),
		kv.RawKey("tiger-beetle"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheStartOfTheRange3(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("accurate"),
		kv.RawKey("storage"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.True(t, iterator.IsValid())

	assert.Equal(t, kv.NewStringKeyWithTimestamp("bolt", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToANonExistingKey(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.RawKey("tiger-beetle"),
		kv.RawKey("tiger-beetle"),
	)
	iterator := NewPendingWritesIterator(batch, 2, keyRange)

	assert.False(t, iterator.IsValid())
}
