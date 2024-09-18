package compact

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/table"
	"go-lsm/test_utility"
	"go-lsm/txn"
	"testing"
	"time"
)

func TestStartSimpleLeveledCompactionWithCompactionDescription(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  rootPath,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    8192,
		CompactionOptions: state.SimpleLeveledCompactionOptions{
			NumberOfSSTablesRatioPercentage: 200,
			MaxLevels:                       3,
			Level0FilesCompactionTrigger:    2,
		},
	}

	storageState, _ := state.NewStorageStateWithOptions(storageOptions)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	buildL0SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.SetSSTableAtLevel(ssTable, 0)
	}
	buildL1SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("bolt", 9), kv.NewStringValue("b+tree"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.SetSSTableAtLevel(ssTable, 1)
	}

	buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	buildL1SSTable(storageState.SSTableIdGenerator().NextId())

	storageStateSnapshot := storageState.Snapshot()

	assert.Equal(t, []uint64{3, 2}, storageStateSnapshot.OrderedSSTableIds(0)) //id 1 is for current memtable
	assert.Equal(t, []uint64{4}, storageStateSnapshot.OrderedSSTableIds(1))

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageOptions)
	storageStateChangeEvent, err := compaction.Start(storageStateSnapshot)

	assert.Nil(t, err)
	assert.Equal(t, -1, storageStateChangeEvent.Description.UpperLevel)
	assert.Equal(t, 1, storageStateChangeEvent.Description.LowerLevel)
	assert.Equal(t, []uint64{4}, storageStateChangeEvent.Description.LowerLevelSSTableIds)
	assert.Equal(t, []uint64{3, 2}, storageStateChangeEvent.Description.UpperLevelSSTableIds)
}

func TestStartSimpleLeveledCompactionBetweenL0AndL1WithSSTablesPresentOnlyInL0(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  rootPath,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    8192,
		CompactionOptions: state.SimpleLeveledCompactionOptions{
			NumberOfSSTablesRatioPercentage: 200,
			MaxLevels:                       3,
			Level0FilesCompactionTrigger:    2,
		},
	}

	storageState, _ := state.NewStorageStateWithOptions(storageOptions)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	buildL0SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.SetSSTableAtLevel(ssTable, 0)
	}

	buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	buildL0SSTable(storageState.SSTableIdGenerator().NextId())

	storageStateSnapshot := storageState.Snapshot()

	assert.Equal(t, []uint64{3, 2}, storageStateSnapshot.OrderedSSTableIds(0)) //id 1 is for current memtable
	assert.Equal(t, []uint64{}, storageStateSnapshot.OrderedSSTableIds(1))

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageOptions)
	storageStateChangeEvent, err := compaction.Start(storageStateSnapshot)
	assert.Nil(t, err)

	newSSTables := storageStateChangeEvent.NewSSTables
	assert.Equal(t, 1, len(newSSTables))

	newSSTable := newSSTables[0]
	iterator, err := newSSTable.SeekToFirst()

	assert.Nil(t, err)
	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "paxos", iterator.Value().String())

	assert.Nil(t, iterator.Next())
	assert.False(t, iterator.IsValid())
}

func TestStartSimpleLeveledCompactionBetweenL0AndL1WithNewSSTables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageOptions := state.StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  rootPath,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    8192,
		CompactionOptions: state.SimpleLeveledCompactionOptions{
			NumberOfSSTablesRatioPercentage: 200,
			MaxLevels:                       3,
			Level0FilesCompactionTrigger:    2,
		},
	}

	storageState, _ := state.NewStorageStateWithOptions(storageOptions)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	buildL0SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.SetSSTableAtLevel(ssTable, 0)
	}
	buildL1SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("bolt", 9), kv.NewStringValue("b+tree"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.SetSSTableAtLevel(ssTable, 1)
	}

	buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	buildL1SSTable(storageState.SSTableIdGenerator().NextId())

	storageStateSnapshot := storageState.Snapshot()

	assert.Equal(t, []uint64{3, 2}, storageStateSnapshot.OrderedSSTableIds(0)) //id 1 is for current memtable
	assert.Equal(t, []uint64{4}, storageStateSnapshot.OrderedSSTableIds(1))

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageOptions)
	storageStateChangeEvent, err := compaction.Start(storageStateSnapshot)
	assert.Nil(t, err)

	newSSTables := storageStateChangeEvent.NewSSTables
	assert.Equal(t, 1, len(newSSTables))

	newSSTable := newSSTables[0]
	iterator, err := newSSTable.SeekToFirst()

	assert.Nil(t, err)
	assert.Equal(t, "bolt", iterator.Key().RawString())
	assert.Equal(t, "b+tree", iterator.Value().String())

	assert.Nil(t, iterator.Next())
	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "paxos", iterator.Value().String())

	assert.Nil(t, iterator.Next())
	assert.False(t, iterator.IsValid())
}
