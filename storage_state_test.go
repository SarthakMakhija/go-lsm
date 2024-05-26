package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestStorageStateWithASinglePutAndHasNotImmutableMemTables(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemTables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))

	value, ok := storageState.Get(txn.NewStringKey("consensus"))

	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	value, ok := storageState.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = storageState.Get(txn.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)

	value, ok = storageState.Get(txn.NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("LSM"), value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	storageState := NewStorageState()
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Delete(txn.NewStringKey("consensus")))

	value, ok := storageState.Get(txn.NewStringKey("consensus"))

	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemTable(t *testing.T) {
	storageState := NewStorageStateWithOptions(StorageOptions{memTableSizeInBytes: 10})
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	assert.True(t, storageState.hasImmutableMemTables())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(StorageOptions{memTableSizeInBytes: 10})
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("B+Tree")))

	value, ok := storageState.Get(txn.NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("B+Tree"), value)
}

func TestStorageStateScan(t *testing.T) {
	storageState := NewStorageState()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveRange(txn.NewStringKey("accurate"), txn.NewStringKey("etcd")))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("data-structure"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleIterators(t *testing.T) {
	storageState := NewStorageState()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveRange(txn.NewStringKey("accurate"), txn.NewStringKey("etcd")))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("data-structure"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}
