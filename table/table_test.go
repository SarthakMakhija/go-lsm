package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableWithASingleBlockContainingSingleKeyValue.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestSSTableWithATwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 20), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableWithATwoBlocks.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.Equal(t, 2, ssTable.noOfBlocks())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("bbolt"))

	directory := "."
	filePath := filepath.Join(directory, "TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairsWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 20), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 30), kv.NewStringValue("bbolt"))

	directory := "."
	filePath := filepath.Join(directory, "TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairsWithStartingAndEndingKey.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)
	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 30), ssTable.endingKey)
}

func TestLoadAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 30), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 40), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestLoadAnSSTableWithTwoBlocks.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestLoadAnSSTableWithTwoBlocksWithStartingAndEndingKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 30), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestLoadAnSSTableWithTwoBlocksWithStartingAndEndingKey.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 20), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 30), ssTable.endingKey)
}

func TestSSTableContainsAGiveInclusiveKeyRange1(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableContainsAGiveInclusiveKeyRange1.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.True(t, ssTable.ContainsInclusive(
		kv.NewInclusiveKeyRange(
			kv.NewStringKeyWithTimestamp("bolt", 2), kv.NewStringKeyWithTimestamp("debt", 6)),
	))
}

func TestSSTableContainsAGiveInclusiveKeyRange2(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 9), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableContainsAGiveInclusiveKeyRange2.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.True(t, ssTable.ContainsInclusive(
		kv.NewInclusiveKeyRange(
			kv.NewStringKeyWithTimestamp("crate", 5), kv.NewStringKeyWithTimestamp("paxos", 20))),
	)
}

func TestSSTableDoesNotContainAGiveInclusiveKeyRange1(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 5), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableDoesNotContainAGiveInclusiveKeyRange1.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.False(t, ssTable.ContainsInclusive(
		kv.NewInclusiveKeyRange(
			kv.NewStringKeyWithTimestamp("bolt", 4), kv.NewStringKeyWithTimestamp("bunt", 8))),
	)
}

func TestSSTableDoesNotContainAGiveInclusiveKeyRange2(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	directory := "."
	filePath := filepath.Join(directory, "TestSSTableDoesNotContainAGiveInclusiveKeyRange2.log")
	defer func() {
		_ = os.Remove(filePath)
	}()

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.False(t, ssTable.ContainsInclusive(
		kv.NewInclusiveKeyRange(
			kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringKeyWithTimestamp("traffik", 6))),
	)
}
