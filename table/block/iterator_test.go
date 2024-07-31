package block

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockSeekWithSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 4), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToFirst()
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 5), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("etcd", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheWithTimestampLesserThanTheProvided(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 5), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("etcd", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 5), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("consensus", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("distributed", 7))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("etcd", 6), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringValue("kv"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("foundationDb", 7), txn.NewStringValue("distributed-kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("distributed", 8))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("etcd", 6), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("foundationDb", 7), iterator.Key())
	assert.Equal(t, txn.NewStringValue("distributed-kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithAnEmptyValue(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.EmptyValue)

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("consensus", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), iterator.Key())
	assert.Equal(t, txn.NewStringValue(""), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKeyWithTimestamp("foundationDb", 7))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}
