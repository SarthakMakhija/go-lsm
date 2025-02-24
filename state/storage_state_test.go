package state

import (
	"errors"
	"go-lsm/kv"
	"go-lsm/table"
	"go-lsm/test_utility"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testStorageStateOptionsWithMemTableSizeAndDirectory(memtableSizeInBytes int64, directory string) StorageOptions {
	return StorageOptions{
		MemTableSizeInBytes:   memtableSizeInBytes,
		Path:                  directory,
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
	}
}

func testStorageStateOptionsWithDirectoryAndCompactionOptions(directory string, compactionStrategyOptions SimpleLeveledCompactionOptions) StorageOptions {
	return StorageOptions{
		MemTableSizeInBytes:   1 << 10,
		Path:                  directory,
		MaximumMemtables:      10,
		FlushMemtableDuration: 1 * time.Minute,
		CompactionOptions: CompactionOptions{
			StrategyOptions: compactionStrategyOptions,
		},
	}
}

func TestStorageStateWithASinglePutAndHasNoImmutableMemtables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))
	assert.False(t, storageState.HasImmutableMemtables())
}

func TestStorageStateWithASinglePutAndGet(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestStorageStateWithAMultiplePutsAndGets(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

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

func TestStorageStateWithSSTablesOnlyAtLevel0(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("etcd", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("distributed", 12))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables1(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("LSM"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingMemtablesAndSSTables3(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("paxos", 10))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingOnlySSTablesAtLevel1(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level1)

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("etcd", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 9))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("distributed", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingOnlySSTablesAtLevel0andLevel1(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))
	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level0)

	ssTableBuilder = table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 3), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("FoundationDb"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("KV"))
	ssTable, err = ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level1)

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("etcd", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("bbolt"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 9))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("distributed", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("TiKV"), value)
}

func TestStorageStateWithAMultiplePutsAndGetsUsingOnlySSTablesAtLevel1andLevel2(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level1)

	ssTableBuilder = table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 3), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 4), kv.NewStringValue("FoundationDb"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 5), kv.NewStringValue("KV"))
	ssTable, err = ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level2)

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("etcd", 8))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("KV"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 9))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("paxos"), value)

	value, ok = storageState.Get(kv.NewStringKeyWithTimestamp("distributed", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("TiKV"), value)
}

func TestStorageStateWithASinglePutAndDelete(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	batch.Delete([]byte("consensus"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("consensus", 11))

	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestStorageStateWithAMultiplePutsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	assert.True(t, storageState.HasImmutableMemtables())
	assert.Equal(t, 3, len(storageState.immutableMemtables))
	assert.Equal(t, []uint64{1, 2, 3, 4}, storageState.sortedMemtableIds())
}

func TestStorageStateWithAMultiplePutsAndGetsInvolvingFreezeOfCurrentMemtable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 6)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("B+Tree"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	value, ok := storageState.Get(kv.NewStringKeyWithTimestamp("data-structure", 10))
	assert.True(t, ok)
	assert.True(t, storageState.HasImmutableMemtables())
	assert.Equal(t, kv.NewStringValue("B+Tree"), value)
}

func TestStorageStateScanWithMemtable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 11)))

	assert.True(t, storageState.HasImmutableMemtables())

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 12), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 13), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 14), kv.NewStringKeyWithTimestamp("distributed", 14)),
	)
	iterator.Close()

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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 20)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 21)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 22)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 9), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 10), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("paxos", 11), kv.NewStringKeyWithTimestamp("quotient", 11)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithSSTablesAtLevel1AndLevel2(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithDirectoryAndCompactionOptions(rootPath, SimpleLeveledCompactionOptions{
		NumberOfSSTablesRatioPercentage: 200,
		MaxLevels:                       4,
		Level0FilesCompactionTrigger:    5,
	}))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))
	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level1)

	ssTableBuilder = table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("JunoDB"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("KV"))
	ssTable, err = ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level2)

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 11), kv.NewStringKeyWithTimestamp("quotient", 11)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanWithSSTablesAtLevel0AndLevel1(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithDirectoryAndCompactionOptions(rootPath, SimpleLeveledCompactionOptions{
		NumberOfSSTablesRatioPercentage: 200,
		MaxLevels:                       4,
		Level0FilesCompactionTrigger:    5,
	}))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("graph", 7), kv.NewStringValue("dGraph"))
	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level0)

	ssTableBuilder = table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 8), kv.NewStringValue("VSR"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 8), kv.NewStringValue("Foundation"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("KV"))
	ssTable, err = ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level0)

	ssTableBuilder = table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("raft"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 6), kv.NewStringValue("JunoDB"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 6), kv.NewStringValue("B+Tree"))
	ssTable, err = ssTableBuilder.Build(3, rootPath)
	assert.Nil(t, err)

	storageState.SetSSTableAtLevel(ssTable, level1)

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 11), kv.NewStringKeyWithTimestamp("quotient", 11)),
	)
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 8), iterator.Key())
	assert.Equal(t, kv.NewStringValue("VSR"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 8), iterator.Key())
	assert.Equal(t, kv.NewStringValue("Foundation"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 8), iterator.Key())
	assert.Equal(t, kv.NewStringValue("KV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("graph", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("dGraph"), iterator.Value())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateScanSSTableAndReferencesToSSTable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 7), kv.NewStringValue("bbolt"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	storageState.l0SSTableIds = append(storageState.l0SSTableIds, 1)
	storageState.ssTables[1] = ssTable

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("paxos", 11), kv.NewStringKeyWithTimestamp("quotient", 11)),
	)
	iterator.Close()

	assert.Equal(t, int64(0), ssTable.TotalReferences())
}

func TestStorageStateScanWithMultipleInvalidIterators(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))
	storageState.forceFreezeCurrentMemtable()

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	iterator := storageState.Scan(
		kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("zen", 10), kv.NewStringKeyWithTimestamp("zen", 10)),
	)
	defer iterator.Close()

	assert.False(t, iterator.IsValid())
}

func TestStorageStateWithZeroImmutableMemtablesAndForceFlushNextImmutableMemtable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(1<<10, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	assert.False(t, storageState.HasImmutableMemtables())
	assert.Panics(t, func() {
		_ = storageState.forceFlushNextImmutableMemtable()
	})
}

func TestStorageStateWithForceFlushNextImmutableMemtable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 7)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	assert.True(t, storageState.HasImmutableMemtables())
	assert.Equal(t, 2, len(storageState.immutableMemtables))

	expected, _ := filepath.Abs(filepath.Join(storageState.WALDirectoryPath(), "1.wal"))
	walPathOfFirstImmutableMemtable, _ := storageState.immutableMemtables[0].WalPath()
	assert.Equal(t, expected, walPathOfFirstImmutableMemtable)

	expected, _ = filepath.Abs(filepath.Join(storageState.WALDirectoryPath(), "2.wal"))
	walPathOfSecondImmutableMemtable, _ := storageState.immutableMemtables[1].WalPath()
	assert.Equal(t, expected, walPathOfSecondImmutableMemtable)

	err := storageState.forceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	_, err = os.Stat(walPathOfFirstImmutableMemtable)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestStorageStateWithForceFlushNextImmutableMemtableAndReadFromSSTable(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(250, rootPath))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	err := storageState.forceFlushNextImmutableMemtable()
	assert.Nil(t, err)

	ssTable, err := table.Load(1, rootPath, 4096)
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  rootPath,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
	}

	storageState, _ := NewStorageStateWithOptions(storageOptions)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 8)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 9)))

	batch = kv.NewBatch()
	_ = batch.Put([]byte("data-structure"), []byte("LSM"))
	assert.Nil(t, storageState.Set(kv.NewTimestampedBatchFrom(*batch, 10)))

	time.Sleep(100 * time.Millisecond)

	ssTable, err := table.Load(1, rootPath, 4096)
	assert.Nil(t, err)

	iterator, err := ssTable.SeekToFirst()
	defer iterator.Close()

	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
