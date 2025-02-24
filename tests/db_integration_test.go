package tests

import (
	go_lsm "go-lsm"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/test_utility"
	"go-lsm/txn"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadWithEmptyDb(t *testing.T) {
	directory := test_utility.SetupADirectoryWithTestName(t)
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
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	err := db.Read(func(transaction *txn.Transaction) {
		_, ok := transaction.Get([]byte("consensus"))
		assert.False(t, ok)
	})
	assert.NoError(t, err)
}

func TestReadAnExistingKeyValue(t *testing.T) {
	directory := test_utility.SetupADirectoryWithTestName(t)
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
		test_utility.CleanupDirectoryWithTestName(t)
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
	directory := test_utility.SetupADirectoryWithTestName(t)
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
		test_utility.CleanupDirectoryWithTestName(t)
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
		defer iterator.Close()

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
	directory := test_utility.SetupADirectoryWithTestName(t)
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
		test_utility.CleanupDirectoryWithTestName(t)
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

func TestScanAndValidateReferencesOfSSTables(t *testing.T) {
	directory := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	executeInTransaction := func(key, value []byte) {
		resultingFuture, err := db.Write(func(transaction *txn.Transaction) {
			assert.NoError(t, transaction.Set(key, value))
		})
		assert.Nil(t, err)
		resultingFuture.Wait()

		assert.True(t, resultingFuture.Status().IsOk())
	}

	executeInTransaction([]byte("raft"), []byte("consensus algorithm"))
	executeInTransaction([]byte("storage"), []byte("Flash SSD"))
	executeInTransaction([]byte("data-structure"), []byte("Buffered B+Tree"))

	time.Sleep(2 * time.Second)
	assert.True(t, db.StorageState().TotalSSTablesAtLevel(0) > 0)

	keyValues, err := db.Scan(kv.NewInclusiveKeyRange(kv.RawKey("raft"), kv.RawKey("wisckey")))

	assert.NoError(t, err)
	assert.Equal(t, []go_lsm.KeyValue{
		{Key: kv.RawKey("raft"), Value: []byte("consensus algorithm")},
		{Key: kv.RawKey("storage"), Value: []byte("Flash SSD")},
	}, keyValues)

	referenceCounts, n := db.StorageState().SSTableReferenceCountAtLevel(0)
	expected := make([]int64, n)
	for index := 0; index < n; index++ {
		expected[index] = 0
	}
	assert.Equal(t, expected, referenceCounts)
}

func TestGetAfterCompaction(t *testing.T) {
	directory := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  directory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    1 * 1024 * 1024,
		CompactionOptions: state.CompactionOptions{
			StrategyOptions: state.SimpleLeveledCompactionOptions{
				NumberOfSSTablesRatioPercentage: 200,
				MaxLevels:                       3,
				Level0FilesCompactionTrigger:    2,
			},
			Duration: 10 * time.Millisecond,
		},
	}
	db, _ := go_lsm.Open(storageOptions)
	defer func() {
		db.Close()
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	runInTransaction := func(key, value []byte) {
		resultingFuture, err := db.Write(func(transaction *txn.Transaction) {
			assert.NoError(t, transaction.Set(key, value))
		})
		assert.NoError(t, err)

		resultingFuture.Wait()
		assert.True(t, resultingFuture.Status().IsOk())
	}

	runInTransaction([]byte("raft"), []byte("consensus algorithm"))
	runInTransaction([]byte("storage"), []byte("Flash SSD"))
	runInTransaction([]byte("disk type"), []byte("NVMe"))
	runInTransaction([]byte("data-structure"), []byte("Buffered BTree"))

	time.Sleep(2 * time.Second)

	assert.Nil(t, db.Read(func(transaction *txn.Transaction) {
		value, ok := transaction.Get([]byte("raft"))
		assert.True(t, ok)
		assert.Equal(t, "consensus algorithm", value.String())
	}))
	assert.Nil(t, db.Read(func(transaction *txn.Transaction) {
		value, ok := transaction.Get([]byte("storage"))
		assert.True(t, ok)
		assert.Equal(t, "Flash SSD", value.String())
	}))
	assert.Nil(t, db.Read(func(transaction *txn.Transaction) {
		value, ok := transaction.Get([]byte("disk type"))
		assert.True(t, ok)
		assert.Equal(t, "NVMe", value.String())
	}))
	assert.Nil(t, db.Read(func(transaction *txn.Transaction) {
		value, ok := transaction.Get([]byte("data-structure"))
		assert.True(t, ok)
		assert.Equal(t, "Buffered BTree", value.String())
	}))
}
