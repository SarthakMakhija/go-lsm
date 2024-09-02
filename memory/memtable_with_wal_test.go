package memory

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
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
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestMemtableWithWALWithMultipleKeys(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	defer func() {
		_ = os.RemoveAll(walDirectoryPath)
	}()

	memTable := NewMemtable(2, testMemtableSize, NewWALPresence(true, walDirectoryPath))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("storage", 6), kv.NewStringValue("NVMe"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = memTable.Get(kv.NewStringKeyWithTimestamp("storage", 6))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)
}

func TestMemtableRecoveryFromWAL(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	defer func() {
		_ = os.RemoveAll(walDirectoryPath)
	}()

	memTable := NewMemtable(3, testMemtableSize, NewWALPresence(true, walDirectoryPath))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("storage", 6), kv.NewStringValue("NVMe"))

	memTable.wal.Close()

	recoveredMemTable, maxTimestamp, err := recoverFromWAL(3, testMemtableSize, filepath.Join(walDirectoryPath, "3.wal"))
	assert.Nil(t, err)

	value, ok := recoveredMemTable.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = recoveredMemTable.Get(kv.NewStringKeyWithTimestamp("storage", 6))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)

	assert.Equal(t, uint64(6), maxTimestamp)
}
