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

func TestStorageStateWithASinglePutAndHasNoImmutableMemtables(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemtables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft")))

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("consensus", 11))

	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("consensus", 6))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = storageState.Get(txn.NewStringKeyWithTimestamp("storage", 8))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)

	value, ok = storageState.Get(txn.NewStringKeyWithTimestamp("data-structure", 9))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 7), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 8), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("etcd", 8))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(txn.NewStringKeyWithTimestamp("consensus", 9))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = storageState.Get(txn.NewStringKeyWithTimestamp("distributed", 10))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables2(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 7), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 8), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 7), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 8), txn.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("paxos", 10))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Delete(txn.NewStringKeyWithTimestamp("consensus", 8)))

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("consensus", 11))

	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(200))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))

	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, 3, len(storageState.immutableMemtables))
	assert.Equal(t, []uint64{1, 2, 3, 4}, storageState.sortedMemtableIds())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(200))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 6), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 7), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 8), txn.NewStringValue("LSM")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 9), txn.NewStringValue("B+Tree")))

	value, ok := storageState.Get(txn.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, txn.NewStringValue("B+Tree"), value)
}

func TestStorageStateScanWithMemtable(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 8), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 9), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("accurate", 10), txn.NewStringKeyWithTimestamp("etcd", 10)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 7), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("data-structure", 9), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleIteratorsAndMemtableOnly(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("raft")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 8), txn.NewStringValue("NVMe")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 9), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(txn.NewInclusiveKeyRange(
		txn.NewStringKeyWithTimestamp("accurate", 10), txn.NewStringKeyWithTimestamp("etcd", 10)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 7), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("data-structure", 9), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables1(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 9), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 10), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 11), txn.NewStringValue("LSM")))

	assert.True(t, storageState.hasImmutableMemtables())

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 12), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 13), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("consensus", 14), txn.NewStringKeyWithTimestamp("distributed", 14)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 9), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("data-structure", 11), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed", 12), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables2(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 21), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 22), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 9), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 10), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("distributed", 23), txn.NewStringKeyWithTimestamp("etcd", 23)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed", 9), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("etcd", 10), iterator.Key())
	assert.Equal(t, txn.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables3(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 9), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 10), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 7), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 7), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables3.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("consensus", 11), txn.NewStringKeyWithTimestamp("elegant", 11)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 8), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("data-structure", 10), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed", 7), iterator.Key())
	assert.Equal(t, txn.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables4(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 9), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 10), txn.NewStringValue("LSM")))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("paxos"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("distributed", 7), txn.NewStringValue("TiKV"))
	ssTableBuilder.Add(txn.NewStringKeyWithTimestamp("etcd", 7), txn.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables4.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("paxos", 11), txn.NewStringKeyWithTimestamp("quotient", 11)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleInvalidIterators(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("raft")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 8), txn.NewStringValue("NVMe")))
	storageState.forceFreezeCurrentMemtable()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 9), txn.NewStringValue("LSM")))

	iterator := storageState.Scan(
		txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("zen", 10), txn.NewStringKeyWithTimestamp("zen", 10)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithZeroImmutableMemtablesAndForceFlushNextImmutableMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(1 << 10))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("raft")))

	assert.False(t, storageState.hasImmutableMemtables())

	assert.Panics(t, func() {
		_ = storageState.ForceFlushNextImmutableMemtable()
	})
}

func TestStorageStateWithForceFlushNextImmutableMemtable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(250, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 7), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 8), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 9), txn.NewStringValue("LSM")))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(250, tempDirectory))
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 9), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 10), txn.NewStringValue("LSM")))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	defer iterator.Close()

	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTableAtFixedInterval(t *testing.T) {
	tempDirectory := os.TempDir()

	storageOptions := StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  tempDirectory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
	}
	storageState := NewStorageStateWithOptions(storageOptions)
	defer storageState.Close()

	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("consensus", 8), txn.NewStringValue("raft")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("storage", 9), txn.NewStringValue("NVMe")))
	storageState.Set(txn.NewTimestampedBatch().Put(txn.NewStringKeyWithTimestamp("data-structure", 10), txn.NewStringValue("LSM")))

	time.Sleep(10 * time.Millisecond)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	defer iterator.Close()

	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
