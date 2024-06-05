package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/table"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testStorageStateOptions(memtableSizeInBytes int64) StorageOptions {
	return StorageOptions{
		MemTableSizeInBytes:   memtableSizeInBytes,
		Path:                  ".",
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
	}
}

func testStorageStateOptionsWithDirectory(memtableSizeInBytes int64, directory string) StorageOptions {
	return StorageOptions{
		MemTableSizeInBytes:   memtableSizeInBytes,
		Path:                  directory,
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
	}
}

func TestStorageStateWithASinglePutAndHasNotImmutableMemtables(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemtables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))

	value, ok := storageState.Get(txn.NewStringKey("consensus"))

	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

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

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKey("etcd"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = storageState.Get(txn.NewStringKey("distributed"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables2(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKey("paxos"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Delete(txn.NewStringKey("consensus")))

	value, ok := storageState.Get(txn.NewStringKey("consensus"))

	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(10))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, 3, len(storageState.immutableMemtables))
	assert.Equal(t, []uint64{1, 2, 3, 4}, storageState.sortedMemtableIds())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(10))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("B+Tree")))

	value, ok := storageState.Get(txn.NewStringKey("data-structure"))
	assert.True(t, ok)
	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, txn.NewStringValue("B+Tree"), value)
}

func TestStorageStateScanWithMemtable(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("accurate"), txn.NewStringKey("etcd")))

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

func TestStorageStateScanWithMultipleIteratorsAndMemtableOnly(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("accurate"), txn.NewStringKey("etcd")))

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

func TestStorageStateScanWithImmutableMemtablesAndSSTables1(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("consensus"), txn.NewStringKey("distributed")))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("data-structure"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables2(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("distributed"), txn.NewStringKey("etcd")))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("etcd"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables3(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("consensus"), txn.NewStringKey("elegant")))

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("data-structure"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKey("distributed"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables4(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKey("consensus"), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKey("distributed"), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKey("etcd"), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "temp.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("paxos"), txn.NewStringKey("quotient")))

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleInvalidIterators(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("zen"), txn.NewStringKey("zen")))
	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithZeroImmutableMemtablesAndForceFlushNextImmutableMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(1 << 10))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemtables())

	assert.Panics(t, func() {
		_ = storageState.ForceFlushNextImmutableMemtable()
	})
}

func TestStorageStateWithForceFlushNextImmutableMemtable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(10, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTableAtFixedInterval(t *testing.T) {
	tempDirectory := os.TempDir()

	storageOptions := StorageOptions{
		MemTableSizeInBytes:   10,
		Path:                  tempDirectory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
	}
	storageState := NewStorageStateWithOptions(storageOptions)
	defer storageState.Close()

	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("storage"), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewBatch().Put(txn.NewStringKey("data-structure"), txn.NewStringValue("LSM")))

	time.Sleep(10 * time.Millisecond)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
