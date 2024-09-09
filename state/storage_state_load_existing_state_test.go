package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/test_utility"
	"testing"
)

func TestStorageStateLoadExistingStateWithMultipleImmutableMemtables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

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

	assert.True(t, storageState.hasImmutableMemtables())

	storageState.Close()

	loadedStorageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))
	assert.True(t, loadedStorageState.hasImmutableMemtables())

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
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

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

	assert.True(t, storageState.hasImmutableMemtables())

	err := storageState.forceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	storageState.Close()
	loadedStorageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

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
