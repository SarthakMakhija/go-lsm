package block

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestBlockSeekWithSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToFirst()
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("etcd", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheWithTimestampLesserThanTheProvided(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("etcd", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 5))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("distributed", 7))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 6), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("foundationDb", 7), kv.NewStringValue("distributed-kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("distributed", 8))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 6), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("foundationDb", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("distributed-kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithAnEmptyValue(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.EmptyValue)

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), iterator.Key())
	assert.Equal(t, kv.NewStringValue(""), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	blockBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(kv.NewStringKeyWithTimestamp("foundationDb", 7))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}
