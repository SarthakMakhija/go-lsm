package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestPendingWritesIteratorWithAnEmptyBatch(t *testing.T) {
	iterator := NewPendingWritesIterator(kv.NewBatch(), 2)
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorWithABatchContainingOneKeyValuePair(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	iterator := NewPendingWritesIterator(batch, 2)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorWithABatchContainingOneDeletedKeyValuePair(t *testing.T) {
	batch := kv.NewBatch()
	batch.Delete([]byte("consensus"))

	iterator := NewPendingWritesIterator(batch, 2)

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

	iterator := NewPendingWritesIterator(batch, 2)

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

func TestPendingWritesIteratorSeekToAMatchingKey(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	iterator := NewPendingWritesIterator(batch, 2)
	iterator.seek([]byte("consensus"))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheSpecifiedKey1(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	iterator := NewPendingWritesIterator(batch, 2)
	iterator.seek([]byte("distributed"))

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheSpecifiedKey2(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	iterator := NewPendingWritesIterator(batch, 2)
	iterator.seek([]byte("cart"))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.Equal(t, kv.NewStringKeyWithTimestamp("storage", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("SSD"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestPendingWritesIteratorSeekToAKeyGreaterThanTheSpecifiedKey3(t *testing.T) {
	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("SSD"))
	_ = batch.Put([]byte("bolt"), []byte("kv"))

	iterator := NewPendingWritesIterator(batch, 2)
	iterator.seek([]byte("accurate"))

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

	iterator := NewPendingWritesIterator(batch, 2)
	iterator.seek([]byte("tigerDb"))

	assert.False(t, iterator.IsValid())
}
