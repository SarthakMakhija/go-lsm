package log

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendToWALForId(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	wal, err := NewWALForId(10, walDirectoryPath)

	assert.Nil(t, err)
	defer func() {
		wal.Close()
		_ = os.RemoveAll(walDirectoryPath)
	}()

	if _, err := os.Stat(filepath.Join(walDirectoryPath, "10.wal")); os.IsNotExist(err) {
		panic("WAL does not exist")
	}
	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft")))
	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("kv", 20), txn.NewStringValue("distributed")))
}

func TestAppendToWAL(t *testing.T) {
	walPath := filepath.Join(os.TempDir(), "TestAppendToWAL.log")
	wal, err := NewWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		wal.Close()
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringValue("raft")))
	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("kv", 30), txn.NewStringValue("distributed")))
}

func TestAppendToWALAndRecoverFromWALPath(t *testing.T) {
	walPath := filepath.Join(os.TempDir(), "TestAppendToWALAndRecoverFromWALPath.log")
	wal, err := NewWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft")))
	assert.Nil(t, wal.Append(txn.NewStringKeyWithTimestamp("kv", 5), txn.NewStringValue("distributed")))

	_ = wal.Sync()
	wal.Close()

	keyValues := make(map[string]string)
	keyTimestamps := make(map[string]uint64)
	_, err = Recover(walPath, func(key txn.Key, value txn.Value) {
		keyValues[key.RawString()] = value.String()
		keyTimestamps[key.RawString()] = key.Timestamp()
	})
	assert.Nil(t, err)

	value, ok := keyValues["consensus"]
	assert.True(t, ok)
	assert.Equal(t, "raft", value)
	assert.Equal(t, keyTimestamps["consensus"], uint64(4))

	value, ok = keyValues["kv"]
	assert.True(t, ok)
	assert.Equal(t, "distributed", value)
	assert.Equal(t, keyTimestamps["kv"], uint64(5))
}
