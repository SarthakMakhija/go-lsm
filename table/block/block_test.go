package block

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestEncodeAndDecodeBlockWithASingleKeyValue(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToFirst()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestEncodeAndDecodeBlockWithTwoKeyValues(t *testing.T) {
	blockBuilder := NewBlockBuilder(1024)
	blockBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	blockBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("kv"))

	block := blockBuilder.Build()
	buffer := block.Encode()

	decodedBlock := DecodeToBlock(buffer)
	iterator := decodedBlock.SeekToFirst()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
