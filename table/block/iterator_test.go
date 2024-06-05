package block

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockSeekWithSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

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
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("etcd"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("consensus"))
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
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("distributed"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))
	blockBuilder.Add(txn.NewStringKey("foundationDb"), txn.NewStringValue("distributed-kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("distributed"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("foundationDb"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("distributed-kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyWithAnEmptyValue(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.EmptyValue)

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("consensus"))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue(""), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	iterator := block.SeekToKey(txn.NewStringKey("foundationDb"))
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}
