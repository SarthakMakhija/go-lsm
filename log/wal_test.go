package log

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/memory"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendToWAL(t *testing.T) {
	walPath := filepath.Join(os.TempDir(), "TestAppendToWAL.log")
	wal, err := NewWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		wal.Close()
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	assert.Nil(t, wal.Append(txn.NewStringKey("kv"), txn.NewStringValue("distributed")))
}

func TestAppendToWALAndRecoverFromWALPath(t *testing.T) {
	walPath := filepath.Join(os.TempDir(), "TestAppendToWALAndRecoverFromWALPath.log")
	wal, err := NewWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	assert.Nil(t, wal.Append(txn.NewStringKey("kv"), txn.NewStringValue("distributed")))

	_ = wal.Sync()
	wal.Close()

	memTable := memory.NewMemtable(1, 1024)
	assert.Nil(t, Recover(walPath, func(key txn.Key, value txn.Value) {
		memTable.Set(key, value)
	}))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("kv"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("distributed"), value)
}
