package log

import (
	"github.com/stretchr/testify/assert"
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

	keyValues := make(map[string]string)
	assert.Nil(t, Recover(walPath, func(key txn.Key, value txn.Value) {
		keyValues[key.String()] = value.String()
	}))

	value, ok := keyValues["consensus"]
	assert.True(t, ok)
	assert.Equal(t, "raft", value)

	value, ok = keyValues["kv"]
	assert.True(t, ok)
	assert.Equal(t, "distributed", value)
}
