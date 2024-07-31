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
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestBuildAnSSTableWithASingleBlockContainingSingleKeyValue.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildAnSSTableWithASingleBlockWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestBuildAnSSTableWithASingleBlockWithStartingAndEndingKey.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), ssTable.endingKey)
}

func TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 7), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValues.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()
	defer blockIterator.Close()

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

func TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValuesWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 7), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValuesWithStartingAndEndingKey.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("etcd", 7), ssTable.endingKey)
}

func TestBuildAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 10), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestBuildAnSSTableWithTwoBlocks.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assertBlockWithASingleKeyValue := func(blockIndex int, value txn.Value) {
		block, err := ssTable.readBlock(blockIndex)
		assert.Nil(t, err)

		blockIterator := block.SeekToFirst()
		defer blockIterator.Close()

		assert.True(t, blockIterator.IsValid())
		assert.Equal(t, value, blockIterator.Value())

		_ = blockIterator.Next()
		assert.False(t, blockIterator.IsValid())
	}

	assertBlockWithASingleKeyValue(0, txn.NewStringValue("raft"))
	assertBlockWithASingleKeyValue(1, txn.NewStringValue("TiKV"))
}
