package log

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/memory"
	"go-lsm/txn"
	"os"
	"testing"
)

func TestAppendToWAL(t *testing.T) {
	walPath := os.TempDir() + "wal_append.log"
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
	walPath := os.TempDir() + "wal_append_recover.log"
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
	assert.Nil(t, RecoverInto(walPath, memTable))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("kv"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("distributed"), value)
}
