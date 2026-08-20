package log

import (
	"errors"
	"go-lsm/kv"
	"os"
	"path/filepath"
	"strings"
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

// TestAppendToWALAndRecoverFromWALPathWithRecordCrossingUint16Boundary is a regression test.
// A record whose encoded key size plus value size sums past 65535 previously caused Recover
// to compute the value's end offset in uint16 arithmetic, wrapping around and producing an
// invalid slice range (start > end), which panics.
func TestAppendToWALAndRecoverFromWALPathWithRecordCrossingUint16Boundary(t *testing.T) {
	walPath := filepath.Join(".", "TestAppendToWALAndRecoverFromWALPathWithRecordCrossingUint16Boundary.log")
	wal, err := newWAL(walPath)

	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(walPath)
	}()

	largeKey := strings.Repeat("k", 40000)
	largeValue := strings.Repeat("v", 30000)
	assert.Nil(t, wal.Append(kv.NewStringKeyWithTimestamp(largeKey, 7), kv.NewStringValue(largeValue)))

	_ = wal.Sync()
	wal.Close()

	keyValues := make(map[string]string)
	_, err = Recover(walPath, func(key kv.Key, value kv.Value) {
		keyValues[key.RawString()] = value.String()
	})
	assert.Nil(t, err)

	value, ok := keyValues[largeKey]
	assert.True(t, ok)
	assert.Equal(t, largeValue, value)
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
