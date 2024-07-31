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
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKeyWithTimestamp("consensus", 5))
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
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("storage", 6), txn.NewStringValue("NVMe"))

	value, ok := memTable.Get(txn.NewStringKeyWithTimestamp("consensus", 6))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKeyWithTimestamp("storage", 6))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}

func TestMemtableRecoveryFromWAL(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	defer func() {
		_ = os.RemoveAll(walDirectoryPath)
	}()

	memTable := NewMemtable(3, testMemtableSize, NewWALPresence(true, walDirectoryPath))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("storage", 6), txn.NewStringValue("NVMe"))

	memTable.wal.Close()

	recoveredMemTable, err := recoverFromWAL(3, testMemtableSize, filepath.Join(walDirectoryPath, "3.wal"))
	assert.Nil(t, err)

	value, ok := recoveredMemTable.Get(txn.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = recoveredMemTable.Get(txn.NewStringKeyWithTimestamp("storage", 6))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}
