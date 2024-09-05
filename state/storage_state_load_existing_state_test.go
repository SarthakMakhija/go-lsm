package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"os"
	"testing"
)

func TestStorageStateWithAMultipleImmutableMemtableAndLoadExistingState(t *testing.T) {
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptions(200))
	defer func() {
		_ = os.RemoveAll(storageState.WALDirectoryPath())
		storageState.DeleteManifest()
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

	loadedStorageState, _ := NewStorageStateWithOptions(testStorageStateOptions(200))
	assert.True(t, loadedStorageState.hasImmutableMemtables())

	defer func() {
		_ = os.RemoveAll(loadedStorageState.WALDirectoryPath())
		loadedStorageState.Close()
		loadedStorageState.DeleteManifest()
	}()

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("storage", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("SSD-HDD"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("B+Tree"), value)
}
