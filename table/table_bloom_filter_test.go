package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/test_utility"
	"testing"
)

func TestLoadSSTableWithSingleBlockAndCheckKeysForExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 4096)

	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("consensus", 8)))
	assert.True(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("distributed", 9)))
	assert.True(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("etcd", 10)))
}

func TestLoadSSTableWithSingleBlockAndCheckKeysForNonExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("bbolt"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 4096)

	assert.Nil(t, err)
	assert.False(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("paxos", 7)))
	assert.False(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("bolt", 7)))
}

func TestLoadAnSSTableWithTwoBlocksAndCheckKeysForExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("TiKV"))

	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	ssTable, err := Load(1, rootPath, 30)
	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("consensus", 7)))
	assert.True(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("distributed", 7)))
	assert.False(t, ssTable.MayContain(kv.NewStringKeyWithTimestamp("etcd", 8)))
}
