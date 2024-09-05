package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildAnSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	directory := "."
	filePath := filepath.Join(directory, "TestBuildAnSSTableWithASingleBlockContainingSingleKeyValue.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildAnSSTableWithASingleBlockWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	directory := "."
	filePath := filepath.Join(directory, "TestBuildAnSSTableWithASingleBlockWithStartingAndEndingKey.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), ssTable.endingKey)
}

func TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	directory := "."
	filePath := filepath.Join(directory, "TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValues.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()
	defer blockIterator.Close()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), blockIterator.Value())

	_ = blockIterator.Next()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValuesWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	directory := "."
	filePath := filepath.Join(directory, "TestBuildAnSSTableWithASingleBlockContainingMultipleKeyValuesWithStartingAndEndingKey.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 7), ssTable.endingKey)
}

func TestBuildAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestBuildAnSSTableWithTwoBlocks.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assertBlockWithASingleKeyValue := func(blockIndex int, value kv.Value) {
		block, err := ssTable.readBlock(blockIndex)
		assert.Nil(t, err)

		blockIterator := block.SeekToFirst()
		defer blockIterator.Close()

		assert.True(t, blockIterator.IsValid())
		assert.Equal(t, value, blockIterator.Value())

		_ = blockIterator.Next()
		assert.False(t, blockIterator.IsValid())
	}

	assertBlockWithASingleKeyValue(0, kv.NewStringValue("raft"))
	assertBlockWithASingleKeyValue(1, kv.NewStringValue("TiKV"))
}
