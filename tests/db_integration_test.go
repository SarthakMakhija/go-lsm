package tests

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadInEmptyDb(t *testing.T) {
	directory := filepath.Join(".", "TestReadInEmptyDb")
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   1 * 1024,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		_ = os.RemoveAll(directory)
	}()

	err := db.Read(func(transaction *txn.Transaction) {
		_, ok := transaction.Get([]byte("consensus"))
		assert.False(t, ok)
	})
	assert.NoError(t, err)
}

func TestReadAnExistingKeyValue(t *testing.T) {
	directory := filepath.Join(".", "TestReadAnExistingKeyValue")
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   1 * 1024,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		_ = os.RemoveAll(directory)
	}()

	future, err := db.Write(func(transaction *txn.Transaction) {
		assert.NoError(t, transaction.Set([]byte("raft"), []byte("consensus algorithm")))
		assert.NoError(t, transaction.Set([]byte("VSR"), []byte("consensus algorithm")))
	})
	assert.NoError(t, err)

	future.Wait()
	assert.True(t, future.Status().IsOk())

	err = db.Read(func(transaction *txn.Transaction) {
		value, ok := transaction.Get([]byte("raft"))
		assert.True(t, ok)
		assert.Equal(t, []byte("consensus algorithm"), value.Bytes())

		value, ok = transaction.Get([]byte("VSR"))
		assert.True(t, ok)
		assert.Equal(t, []byte("consensus algorithm"), value.Bytes())
	})
	assert.NoError(t, err)
}

func TestScanKeyValues1(t *testing.T) {
	directory := filepath.Join(".", "TestScanKeyValues1")
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   1 * 1024,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		_ = os.RemoveAll(directory)
	}()

	future, err := db.Write(func(transaction *txn.Transaction) {
		assert.NoError(t, transaction.Set([]byte("raft"), []byte("consensus algorithm")))
		assert.NoError(t, transaction.Set([]byte("vsr"), []byte("consensus algorithm")))
		assert.NoError(t, transaction.Set([]byte("wisckey"), []byte("modified LSM")))
	})
	assert.NoError(t, err)

	future.Wait()
	assert.True(t, future.Status().IsOk())

	err = db.Read(func(transaction *txn.Transaction) {
		iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("storage"), kv.RawKey("wisckey")))

		assert.Equal(t, "vsr", iterator.Key().RawString())
		assert.Equal(t, "consensus algorithm", iterator.Value().String())

		_ = iterator.Next()

		assert.Equal(t, "wisckey", iterator.Key().RawString())
		assert.Equal(t, "modified LSM", iterator.Value().String())

		_ = iterator.Next()
		assert.False(t, iterator.IsValid())
	})
	assert.NoError(t, err)
}

func TestScanKeyValues2(t *testing.T) {
	directory := filepath.Join(".", "TestScanKeyValues2")
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   1 * 1024,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		_ = os.RemoveAll(directory)
	}()

	future, err := db.Write(func(transaction *txn.Transaction) {
		assert.NoError(t, transaction.Set([]byte("raft"), []byte("consensus algorithm")))
		assert.NoError(t, transaction.Set([]byte("vsr"), []byte("consensus algorithm")))
		assert.NoError(t, transaction.Set([]byte("wisckey"), []byte("modified LSM")))
	})
	assert.NoError(t, err)

	future.Wait()
	assert.True(t, future.Status().IsOk())

	keyValues, err := db.Scan(kv.NewInclusiveKeyRange(kv.RawKey("storage"), kv.RawKey("wisckey")))

	assert.NoError(t, err)
	assert.Equal(t, []go_lsm.KeyValue{
		{Key: kv.RawKey("vsr"), Value: []byte("consensus algorithm")},
		{Key: kv.RawKey("wisckey"), Value: []byte("modified LSM")},
	}, keyValues)
}
