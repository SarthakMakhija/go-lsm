package table

import (
	"go-lsm/kv"
	"go-lsm/test_utility"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	block, err := ssTable.readBlock(0)
	assert.Nil(t, err)

	blockIterator := block.SeekToFirst()

	assert.True(t, blockIterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), blockIterator.Value())

	_ = blockIterator.Next()
	assert.False(t, blockIterator.IsValid())
}

func TestSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 20), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	assert.Equal(t, 2, ssTable.noOfBlocks())
}

func TestLoadSSTableWithSingleBlockContainingMultipleKeyValuePairs(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 4096)
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

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 4096)
	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 10), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 30), ssTable.endingKey)
}

func TestLoadAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 30), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 40), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 30)
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
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 30), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 30)
	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 20), ssTable.startingKey)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 30), ssTable.endingKey)
}

func TestSSTableContainsAGiveInclusiveKeyRange1(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	assert.False(t, ssTable.ContainsInclusive(
		kv.NewInclusiveKeyRange(
			kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringKeyWithTimestamp("traffik", 6))),
	)
}

func TestRemoveSSTable(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)
	assert.Nil(t, ssTable.Remove())
}
