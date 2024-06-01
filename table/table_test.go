package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
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

func TestSSTableWithATwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.Equal(t, 2, ssTable.noOfBlocks())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairsWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)
	assert.Nil(t, err)
	assert.Equal(t, txn.NewStringKey("consensus"), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKey("etcd"), ssTable.endingKey)
}

func TestLoadAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestLoadAnSSTableWithTwoBlocksWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)
	assert.Equal(t, txn.NewStringKey("consensus"), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKey("distributed"), ssTable.endingKey)
}
