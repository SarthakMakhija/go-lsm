package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildAnSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("TiKV"), blockIterator.Value())

	_ = blockIterator.Next()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("bbolt"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assertBlockWithASingleKeyValue := func(blockIndex int, value txn.Value) {
		block, err := ssTable.readBlock(blockIndex)
		assert.Nil(t, err)

		blockIterator := block.SeekToFirst()

		assert.True(t, blockIterator.IsValid())
		assert.Equal(t, value, blockIterator.Value())

		_ = blockIterator.Next()
		assert.False(t, blockIterator.IsValid())
	}

	assertBlockWithASingleKeyValue(0, txn.NewStringValue("raft"))
	assertBlockWithASingleKeyValue(1, txn.NewStringValue("TiKV"))
}