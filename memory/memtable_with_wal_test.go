package memory

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestMemtableWithWALWithASingleKey(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	defer func() {
		_ = os.RemoveAll(walDirectoryPath)
	}()

	memTable := NewMemtable(1, testMemtableSize, NewWALPresence(true, walDirectoryPath))
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestMemtableWithWALWithMultipleKeys(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	defer func() {
		_ = os.RemoveAll(walDirectoryPath)
	}()

	memTable := NewMemtable(2, testMemtableSize, NewWALPresence(true, walDirectoryPath))
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("storage"), txn.NewStringValue("NVMe"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}
