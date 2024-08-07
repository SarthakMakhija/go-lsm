package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/table"
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

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))
	assert.False(t, storageState.hasImmutableMemtables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("storage", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 9))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("etcd", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 9))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("distributed", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables2(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("paxos", 10))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	batch.Delete([]byte("consensus"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))

	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(200))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, 3, len(storageState.immutableMemtables))
	assert.Equal(t, []uint64{1, 2, 3, 4}, storageState.sortedMemtableIds())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(200))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("B+Tree"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.True(t, storageState.hasImmutableMemtables())
	assert.Equal(t, kv.NewStringValue("B+Tree"), value)
}

func TestStorageStateScanWithMemtable(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	iterator := storageState.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("accurate", 10), kv.NewStringKeyWithTimestamp("etcd", 10)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("data-structure", 9), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleIteratorsAndMemtableOnly(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	iterator := storageState.Scan(kv.NewInclusiveKeyRange(
		kv.NewStringKeyWithTimestamp("accurate", 10), kv.NewStringKeyWithTimestamp("etcd", 10)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("data-structure", 9), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables1(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 11))

	assert.True(t, storageState.hasImmutableMemtables())

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 12), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 13), kv.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables1.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 14), kv.NewStringKeyWithTimestamp("distributed", 14)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 9), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("data-structure", 11), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 12), iterator.Key())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables2(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 20))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 21))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 22))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 9), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables2.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("distributed", 23), kv.NewStringKeyWithTimestamp("etcd", 23)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 9), iterator.Key())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 10), iterator.Key())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables3(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables3.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 11), kv.NewStringKeyWithTimestamp("elegant", 11)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 8), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("data-structure", 10), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithImmutableMemtablesAndSSTables4(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(200, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	filePath := filepath.Join(tempDirectory, "TestStorageStateScanWithImmutableMemtablesAndSSTables4.log")

	ssTable, err := ssTableBuilder.Build(1, filePath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("paxos", 11), kv.NewStringKeyWithTimestamp("quotient", 11)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithMultipleInvalidIterators(t *testing.T) {
	storageState := NewStorageState()
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("zen", 10), kv.NewStringKeyWithTimestamp("zen", 10)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithZeroImmutableMemtablesAndForceFlushNextImmutableMemtable(t *testing.T) {
	storageState := NewStorageStateWithOptions(testStorageStateOptions(1 << 10))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	assert.False(t, storageState.hasImmutableMemtables())
	assert.Panics(t, func() {
		_ = storageState.ForceFlushNextImmutableMemtable()
	})
}

func TestStorageStateWithForceFlushNextImmutableMemtable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(250, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTable(t *testing.T) {
	tempDirectory := os.TempDir()

	storageState := NewStorageStateWithOptions(testStorageStateOptionsWithDirectory(250, tempDirectory))
	defer storageState.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	err := storageState.ForceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	defer iterator.Close()

	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

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

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10))

	time.Sleep(10 * time.Millisecond)

	ssTable, err := table.Load(1, filepath.Join(tempDirectory, "1.sst"), 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	defer iterator.Close()

	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
