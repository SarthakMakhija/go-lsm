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
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockAndCheckKeysForExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)

	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(txn.NewStringKey("consensus")))
	assert.True(t, ssTable.MayContain(txn.NewStringKey("distributed")))
	assert.True(t, ssTable.MayContain(txn.NewStringKey("etcd")))
}

func TestLoadSSTableWithSingleBlockAndCheckKeysForNonExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadSSTableWithSingleBlockAndCheckKeysForNonExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 4096)

	assert.Nil(t, err)
	assert.False(t, ssTable.MayContain(txn.NewStringKey("paxos")))
	assert.False(t, ssTable.MayContain(txn.NewStringKey("bolt")))
}

func TestLoadAnSSTableWithTwoBlocksAndCheckKeysForExistenceUsingBloom(t *testing.T) {
	ssTableBuilder := NewSSTableBuilder(30)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestLoadAnSSTableWithTwoBlocksAndCheckKeysForExistenceUsingBloom.log")

	_, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	ssTable, err := Load(1, filePath, 30)
	assert.Nil(t, err)
	assert.True(t, ssTable.MayContain(txn.NewStringKey("consensus")))
	assert.True(t, ssTable.MayContain(txn.NewStringKey("distributed")))
	assert.False(t, ssTable.MayContain(txn.NewStringKey("etcd")))
}
