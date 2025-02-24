package log

import (
	"errors"
	"go-lsm/kv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendToWALWithAnId(t *testing.T) {
	walDirectoryPath := filepath.Join(".", "wal")
	assert.Nil(t, os.MkdirAll(walDirectoryPath, os.ModePerm))

	wal, err := NewWAL(10, walDirectoryPath)

	assert.Nil(t, err)
	defer func() {
		wal.Close()
		_ = os.RemoveAll(walDirectoryPath)
	}()

	if _, err := os.Stat(filepath.Join(walDirectoryPath, "10.wal")); os.IsNotExist(err) {
		panic("WAL does not exist")
	}
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft")))
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("kv", 20), kv.NewStringValue("distributed")))
}

func TestAppendToWAL(t *testing.T) {
	walPath := filepath.Join(".", "TestAppendToWAL.log")
	wal, err := newWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		wal.Close()
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft")))
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("kv", 30), kv.NewStringValue("distributed")))
}

func TestAppendToWALAndRecoverFromWALPath(t *testing.T) {
	walPath := filepath.Join(".", "TestAppendToWALAndRecoverFromWALPath.log")
	wal, err := newWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft")))
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("kv", 5), kv.NewStringValue("distributed")))

	_ = wal.Sync()
	wal.Close()

	keyValues := make(map[string]string)
	keyTimestamps := make(map[string]uint64)
	_, err = Recover(walPath, func(key kv.Key, value kv.Value) {
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

func TestDeleteWALFile(t *testing.T) {
	walPath := filepath.Join(".", "TestDeleteWALFile.log")
	wal, err := newWAL(walPath)

	assert.Nil(t, err)
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("raft")))
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp("kv", 30), kv.NewStringValue("distributed")))

	wal.DeleteFile()

	_, err = os.Stat(walPath)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestWALPath(t *testing.T) {
	walPath := filepath.Join(".", "TestWALPath.log")
	wal, err := newWAL(walPath)
	defer func() {
		_ = os.Remove(walPath)
	}()

	assert.Nil(t, err)

	absolute, err := filepath.Abs(walPath)
	assert.Nil(t, err)

	path, err := wal.Path()
	assert.Nil(t, err)
	assert.Equal(t, absolute, path)
}
