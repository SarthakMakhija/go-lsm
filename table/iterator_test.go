package table

import (
	"go-lsm/kv"
	"go-lsm/test_utility"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterateOverAnSSTableWithASingleBlockContainingSingleKeyValue(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverAnSSTableWithASingleBlockContainingMultipleKeyValues(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 5), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	defer iterator.Close()

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

func TestIterateOverAnSSTableWithTwoBlocks(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 9), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestSeekToAKeyInSSTableAndCheckTheReferences(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Nil(t, err)
	defer iterator.Close()

	assert.Equal(t, int64(1), ssTable.TotalReferences())
}

func TestSeekToAKeyInSSTableAndCheckTheReferencesAfterDecrementing(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Nil(t, err)
	defer iterator.Close()

	assert.Equal(t, int64(1), ssTable.TotalReferences())

	DecrementReferenceFor([]*SSTable{ssTable})
	assert.Equal(t, int64(0), ssTable.TotalReferences())
}

func TestIterateOverAnSSTableWithASingleBlockContainingSingleKeyValueUsingSeekToKeyGreaterOrEqualToTheGivenKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverAnSSTableWithASingleBlockContainingMultipleKeyValuesUsingSeekToKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("contribute", 9))
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverAnSSTableWithASingleBlockContainingMultipleKeyValuesUsingSeekToKeyContainingTheKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Nil(t, err)

	defer iterator.Close()

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

func TestIterateOverAnSSTableWithTwoBlocksUsingSeekToKey(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("cart", 5), kv.NewStringValue("draft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 10))
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestIterateOverAnSSTableWithTwoBlocksUsingSeekToKeyWithTheKeyLessThanTheFirstKeyOfTheFirstBlock(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(50)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("cart", 9), kv.NewStringValue("draft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 10), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("bolt", 11))
	assert.Nil(t, err)

	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("draft"), iterator.Value())

	_ = iterator.Next()
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
