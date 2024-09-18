package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/compact/meta"
	"go-lsm/kv"
	"go-lsm/table"
	"go-lsm/table/block"
	"go-lsm/test_utility"
	"testing"
)

func TestAllSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds(t *testing.T) {
	event := StorageStateChangeEvent{
		description: meta.SimpleLeveledCompactionDescription{
			UpperLevelSSTableIds: []uint64{1, 2, 3, 4},
		},
	}
	excludedSSTableIds := event.allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds([]uint64{1, 2, 3, 4, 5, 6})
	assert.Equal(t, []uint64{5, 6}, excludedSSTableIds)
}

func TestStorageStateChangeEventByOpeningSSTables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	ssTableBuilder := table.NewSSTableBuilder(block.DefaultBlockSize)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
	ssTable, err := ssTableBuilder.Build(2, rootPath)
	assert.Nil(t, err)

	storageStateChangeEvent, err := NewStorageStateChangeEventByOpeningSSTables(
		[]uint64{ssTable.Id()},
		meta.SimpleLeveledCompactionDescription{},
		rootPath,
	)
	assert.Nil(t, err)
	assert.Equal(t, []uint64{ssTable.Id()}, storageStateChangeEvent.NewSSTableIds)
	assert.Equal(t, 1, len(storageStateChangeEvent.NewSSTables))
}

func TestStorageStateChangeEventByOpeningSSTablesForASSTableIdWhichDoesNotExist(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
	}()

	_, err := NewStorageStateChangeEventByOpeningSSTables(
		[]uint64{2},
		meta.SimpleLeveledCompactionDescription{},
		rootPath,
	)
	assert.Error(t, err)
}
