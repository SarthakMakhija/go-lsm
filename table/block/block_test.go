package block

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestEncodeAndDecodeBlockWithASingleKeyValue(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToFirst()
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithTwoKeyValues(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToFirst()
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
