package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStorageStateWithASinglePutAndHasNotImmutableMemtables(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemTables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))

	value, ok := storageState.Get(NewStringKey("consensus"))

	assert.True(t, ok)
	assert.Equal(t, NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))
	storageState.Set(NewBatch().Put(NewStringKey("storage"), NewStringValue("NVMe")))
	storageState.Set(NewBatch().Put(NewStringKey("data-structure"), NewStringValue("LSM")))

	value, ok := storageState.Get(NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("raft"), value)

	value, ok = storageState.Get(NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("NVMe"), value)

	value, ok = storageState.Get(NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("LSM"), value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))
	storageState.Set(NewBatch().Delete(NewStringKey("consensus")))

	value, ok := storageState.Get(NewStringKey("consensus"))

	assert.False(t, ok)
	assert.Equal(t, emptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(StorageOptions{memTableSizeInBytes: 10})
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))
	storageState.Set(NewBatch().Put(NewStringKey("storage"), NewStringValue("NVMe")))
	storageState.Set(NewBatch().Put(NewStringKey("data-structure"), NewStringValue("LSM")))

	assert.True(t, storageState.hasImmutableMemTables())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(StorageOptions{memTableSizeInBytes: 10})
	storageState.Set(NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft")))
	storageState.Set(NewBatch().Put(NewStringKey("storage"), NewStringValue("NVMe")))
	storageState.Set(NewBatch().Put(NewStringKey("data-structure"), NewStringValue("LSM")))
	storageState.Set(NewBatch().Put(NewStringKey("data-structure"), NewStringValue("B+Tree")))

	value, ok := storageState.Get(NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("B+Tree"), value)
}
