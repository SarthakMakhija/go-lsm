package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/table"
	"go-lsm/test_utility"
	"testing"
)

const level1 = 1

func TestApplyStorageStateChangeEventWhichCompactsAllTheTablesAtLevel0(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := NewStorageState(rootPath)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
	}()

	buildL0SSTable := func(id uint64) table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable

		return ssTable
	}
	buildNewSSTable := func(id uint64) table.SSTable {
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
		upperLevel:           -1,
		upperLevelSSTableIds: []uint64{ssTable.Id(), anotherSSTable.Id()},
		lowerLevel:           1,
		lowerLevelSSTableIds: []uint64{},
		newSSTables:          []table.SSTable{newSSTable},
		newSSTableIds:        []uint64{newSSTable.Id()},
	}
	ssTablesToRemove := storageState.Apply(event)

	assert.Equal(t, 2, len(ssTablesToRemove))
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

	buildL0SSTable := func(id uint64) table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable

		return ssTable
	}
	buildNewSSTable := func(id uint64) table.SSTable {
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
		upperLevel:           -1,
		upperLevelSSTableIds: []uint64{ssTable.Id()},
		lowerLevel:           1,
		lowerLevelSSTableIds: []uint64{},
		newSSTables:          []table.SSTable{newSSTable},
		newSSTableIds:        []uint64{newSSTable.Id()},
	}
	ssTablesToRemove := storageState.Apply(event)

	assert.Equal(t, 1, len(ssTablesToRemove))
	assert.False(t, storageState.hasSSTableWithId(ssTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(anotherSSTable.Id()))
	assert.True(t, storageState.hasSSTableWithId(newSSTable.Id()))
	assert.Equal(t, 1, len(storageState.l0SSTableIds))
	assert.Equal(t, anotherSSTable.Id(), storageState.l0SSTableIds[0])

	assert.Equal(t, 1, len(storageState.levels[level1-1].SSTableIds))
	assert.Equal(t, newSSTable.Id(), storageState.levels[level1-1].SSTableIds[0])
}
