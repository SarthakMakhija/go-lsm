package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockSeekWithSeekToTheFirstKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.build()
	iterator := SeekToFirst(block)

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
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("etcd"))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheMatchingKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("consensus"))

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
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("distributed"))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheKeyGreaterThanTheSpecifiedKeyFollowedByNext(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))
	blockBuilder.add(txn.NewStringKey("foundationDb"), txn.NewStringValue("distributed-kv"))

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("distributed"))

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
	blockBuilder.add(txn.NewStringKey("consensus"), txn.EmptyValue)

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("consensus"))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue(""), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestBlockSeekToTheNonExistingKey(t *testing.T) {
	blockBuilder := NewBlockBuilder(4096)
	blockBuilder.add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.build()
	iterator := SeekToKey(block, txn.NewStringKey("foundationDb"))

	assert.False(t, iterator.IsValid())
}
