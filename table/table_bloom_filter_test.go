package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSSTableWithSingleBlockAndCheckKeysForExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 7), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockAndCheckKeysForExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)

	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("consensus", 8)))
	assert.True(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("distributed", 9)))
	assert.True(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("etcd", 10)))
}

func TestLoadSSTableWithSingleBlockAndCheckKeysForNonExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 6), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockAndCheckKeysForNonExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)

	assert.Nil(t, err)
	assert.False(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("paxos", 7)))
	assert.False(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("bolt", 7)))
}

func TestLoadAnSSTableWithTwoBlocksAndCheckKeysForExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 6), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadAnSSTableWithTwoBlocksAndCheckKeysForExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("consensus", 7)))
	assert.True(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("distributed", 7)))
	assert.False(t, ssTable.MayContain(txn.NewStringKeyWithTimestamp("etcd", 8)))
}
