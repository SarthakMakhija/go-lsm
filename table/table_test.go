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
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableWithASingleBlockContainingSingleKeyValue.log")

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
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 20), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableWithATwoBlocks.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.Equal(t, 2, ssTable.noOfBlocks())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 4), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 4), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs.log")

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
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 20), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 30), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairsWithStartingAndEndingKey.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)
	assert.Nil(t, err)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 10), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("etcd", 30), ssTable.endingKey)
}

func TestLoadAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 30), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 40), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadAnSSTableWithTwoBlocks.log")

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
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 30), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadAnSSTableWithTwoBlocksWithStartingAndEndingKey.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 20), ssTable.startingKey)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed", 30), ssTable.endingKey)
}

func TestSSTableContainsAGiveInclusiveKeyRange1(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableContainsAGiveInclusiveKeyRange1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.True(t, ssTable.ContainsInclusive(
		txn.NewInclusiveKeyRange(
			txn.NewStringKeyWithTimestamp("bolt", 2), txn.NewStringKeyWithTimestamp("debt", 6)),
	))
}

func TestSSTableContainsAGiveInclusiveKeyRange2(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 9), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 10), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableContainsAGiveInclusiveKeyRange2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.True(t, ssTable.ContainsInclusive(
		txn.NewInclusiveKeyRange(
			txn.NewStringKeyWithTimestamp("crate", 5), txn.NewStringKeyWithTimestamp("paxos", 20))),
	)
}

func TestSSTableDoesNotContainAGiveInclusiveKeyRange1(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 5), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableDoesNotContainAGiveInclusiveKeyRange1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.False(t, ssTable.ContainsInclusive(
		txn.NewInclusiveKeyRange(
			txn.NewStringKeyWithTimestamp("bolt", 4), txn.NewStringKeyWithTimestamp("bunt", 8))),
	)
}

func TestSSTableDoesNotContainAGiveInclusiveKeyRange2(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestSSTableDoesNotContainAGiveInclusiveKeyRange2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	assert.False(t, ssTable.ContainsInclusive(
		txn.NewInclusiveKeyRange(
			txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringKeyWithTimestamp("traffik", 6))),
	)
}
