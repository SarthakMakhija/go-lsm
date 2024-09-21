package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/compact/meta"
	"go-lsm/kv"
	"go-lsm/table"
	"go-lsm/test_utility"
	"testing"
)

const (
	level0 = 0
	level1 = 1
	level2 = 2
)

func TestApplyStorageStateChangeEventWhichCompactsAllTheTablesAtLevel0(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	buildL0SSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable

		return ssTable
	}
	buildNewSSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		return ssTable
	}

	ssTable := buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	anotherSSTable := buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	newSSTable := buildNewSSTable(storageState.SSTableIdGenerator().NextId())

	event := StorageStateChangeEvent{
		description: meta.SimpleLeveledCompactionDescription{
			UpperLevel:           -1,
			UpperLevelSSTableIds: []uint64{ssTable.Id(), anotherSSTable.Id()},
			LowerLevel:           1,
			LowerLevelSSTableIds: []uint64{},
		},
		NewSSTables:   []*table.SSTable{newSSTable},
		NewSSTableIds: []uint64{newSSTable.Id()},
	}
	err := storageState.Apply(event, false)

	assert.Nil(t, err)
	assert.False(t, storageState.hasSSTableWithId(ssTable.Id()))
	assert.False(t, storageState.hasSSTableWithId(anotherSSTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(newSSTable.Id()))
	assert.Equal(t, 0, len(storageState.l0SSTableIds))

	assert.Equal(t, 1, len(storageState.levels[level1-1].SSTableIds))
	assert.Equal(t, newSSTable.Id(), storageState.levels[level1-1].SSTableIds[0])
}

func TestApplyStorageStateChangeEventWhichCompactsAllTheTablesAtLevel0ButAnotherSSTableGetsAddedAtLevel0(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	buildL0SSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable

		return ssTable
	}
	buildNewSSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		return ssTable
	}

	ssTable := buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	anotherSSTable := buildL0SSTable(storageState.SSTableIdGenerator().NextId())
	newSSTable := buildNewSSTable(storageState.SSTableIdGenerator().NextId())

	event := StorageStateChangeEvent{
		description: meta.SimpleLeveledCompactionDescription{
			UpperLevel:           -1,
			UpperLevelSSTableIds: []uint64{ssTable.Id()},
			LowerLevel:           1,
			LowerLevelSSTableIds: []uint64{},
		},
		NewSSTables:   []*table.SSTable{newSSTable},
		NewSSTableIds: []uint64{newSSTable.Id()},
	}
	err := storageState.Apply(event, false)

	assert.Nil(t, err)
	assert.False(t, storageState.hasSSTableWithId(ssTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(anotherSSTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(newSSTable.Id()))
	assert.Equal(t, 1, len(storageState.l0SSTableIds))
	assert.Equal(t, anotherSSTable.Id(), storageState.l0SSTableIds[0])

	assert.Equal(t, 1, len(storageState.levels[level1-1].SSTableIds))
	assert.Equal(t, newSSTable.Id(), storageState.levels[level1-1].SSTableIds[0])
}

func TestApplyStorageStateChangeEventWhichCompactsAllTheTablesAtLevel1(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	buildL1SSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.ssTables[id] = ssTable
		storageState.levels[level1-1].SSTableIds = append(storageState.levels[level1-1].SSTableIds, id)

		return ssTable
	}
	buildNewSSTable := func(id uint64) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		return ssTable
	}

	ssTable := buildL1SSTable(storageState.SSTableIdGenerator().NextId())
	anotherSSTable := buildL1SSTable(storageState.SSTableIdGenerator().NextId())
	newSSTable := buildNewSSTable(storageState.SSTableIdGenerator().NextId())

	event := StorageStateChangeEvent{
		description: meta.SimpleLeveledCompactionDescription{
			UpperLevel:           1,
			UpperLevelSSTableIds: []uint64{ssTable.Id(), anotherSSTable.Id()},
			LowerLevel:           2,
			LowerLevelSSTableIds: []uint64{},
		},
		NewSSTables:   []*table.SSTable{newSSTable},
		NewSSTableIds: []uint64{newSSTable.Id()},
	}
	err := storageState.Apply(event, false)

	assert.Nil(t, err)
	assert.False(t, storageState.hasSSTableWithId(ssTable.Id()))
	assert.False(t, storageState.hasSSTableWithId(anotherSSTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(newSSTable.Id()))

	assert.Equal(t, 0, len(storageState.levels[level1-1].SSTableIds))
	assert.Equal(t, newSSTable.Id(), storageState.levels[level2-1].SSTableIds[0])
}
