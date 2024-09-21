package tests

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/compact"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/test_utility"
	"go-lsm/txn"
	"testing"
	"time"
)

func testStorageStateOptionsWithMemTableSizeAndDirectory(memtableSizeInBytes int64, directory string) state.StorageOptions {
	return state.StorageOptions{
		MemTableSizeInBytes:   memtableSizeInBytes,
		Path:                  directory,
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
	}
}

func testStorageStateOptionsWithCompactionOptions(memtableSizeInBytes int64, directory string) state.StorageOptions {
	return state.StorageOptions{
		MemTableSizeInBytes:   memtableSizeInBytes,
		Path:                  directory,
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
		SSTableSizeInBytes:    1 * 1024 * 1024 * 1024,
		CompactionOptions: state.SimpleLeveledCompactionOptions{
			NumberOfSSTablesRatioPercentage: 200,
			MaxLevels:                       3,
			Level0FilesCompactionTrigger:    2,
		},
	}
}

func TestStorageStateLoadExistingStateWithMultipleImmutableMemtables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("SSD-HDD"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("B+Tree"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	assert.True(t, storageState.HasImmutableMemtables())

	storageState.Close()

	loadedStorageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))
	assert.True(t, loadedStorageState.HasImmutableMemtables())

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		loadedStorageState.Close()
	}()

	value, ok := loadedStorageState.Get(kv.NewStringKeyWithTimestamp("consensus", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("storage", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("SSD-HDD"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("B+Tree"), value)
}

func TestStorageStateLoadExistingStateWithSSTable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("Flash SSD"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("Buffered B-Tree"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	assert.True(t, storageState.HasImmutableMemtables())

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	storageState.Close()
	loadedStorageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		loadedStorageState.Close()
	}()

	value, ok := loadedStorageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("storage", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("Flash SSD"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("Buffered B-Tree"), value)
}

func TestStorageStateLoadExistingStateAfterCompaction(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithCompactionOptions(250, rootPath))

	oracle := txn.NewOracle(txn.NewExecutor(storageState))
	compaction := compact.NewCompaction(oracle, storageState.SSTableIdGenerator(), storageState.Options())

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		oracle.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("Flash SSD"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("Buffered B-Tree"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	assert.True(t, storageState.HasImmutableMemtables())
	assert.True(t, storageState.TotalImmutableMemtables() > 1)

	assert.Nil(t, storageState.ForceFlushNextImmutableMemtable())
	assert.Nil(t, storageState.ForceFlushNextImmutableMemtable())

	assert.True(t, storageState.TotalSSTablesAtLevel(0) > 1)

	stateChangeEvent, err := compaction.Start(storageState.Snapshot())
	assert.Nil(t, err)

	assert.Equal(t, -1, stateChangeEvent.CompactionUpperLevel())
	assert.Equal(t, 1, stateChangeEvent.CompactionLowerLevel())

	err = storageState.Apply(stateChangeEvent, false)
	assert.Nil(t, err)

	storageState.Close()
	loadedStorageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithCompactionOptions(250, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		loadedStorageState.Close()
	}()

	assert.True(t, loadedStorageState.TotalSSTablesAtLevel(1) >= 1)

	value, ok := loadedStorageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("storage", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("Flash SSD"), value)

	value, ok = loadedStorageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("Buffered B-Tree"), value)
}
