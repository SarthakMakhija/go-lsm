package tests

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/table"
	"go-lsm/test_utility"
	"testing"
	"time"
)

func TestDoesNotCleanAnSSTableWhichIsBeingReferenced(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))
	ssTableCleaner := table.NewSSTableCleaner(2 * time.Millisecond)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		ssTableCleaner.Stop()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	_, err = ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.Nil(t, err)

	ssTableCleaner.Start()
	ssTableCleaner.Submit([]*table.SSTable{ssTable})
	time.Sleep(1 * time.Second)

	assert.Equal(t, 1, len(ssTableCleaner.PendingSSTablesToClean()))
	assert.Equal(t, ssTable.Id(), ssTableCleaner.PendingSSTablesToClean()[0].Id())
}

func TestCleanTheSSTableWhichIsNotBeingReferenced(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))
	ssTableCleaner := table.NewSSTableCleaner(2 * time.Millisecond)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		ssTableCleaner.Stop()
	}()

	ssTableBuilder := table.NewSSTableBuilder(4096)
	ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))

	ssTable, err := ssTableBuilder.Build(1, rootPath)
	assert.Nil(t, err)

	_, err = ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 11))
	assert.Nil(t, err)

	table.DecrementReferenceFor([]*table.SSTable{ssTable})

	ssTableCleaner.Start()
	ssTableCleaner.Submit([]*table.SSTable{ssTable})
	time.Sleep(1 * time.Second)

	assert.Equal(t, 0, len(ssTableCleaner.PendingSSTablesToClean()))
}

func TestCleanAnSSTableOutOfMultipleSSTables(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageStateWithOptions(testStorageStateOptionsWithMemTableSizeAndDirectory(200, rootPath))
	ssTableCleaner := table.NewSSTableCleaner(2 * time.Millisecond)

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		ssTableCleaner.Stop()
	}()

	buildAnSSTable := func(id uint64, seekToKey bool) *table.SSTable {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringValue("paxos"))

		ssTable, err := ssTableBuilder.Build(id, rootPath)
		assert.Nil(t, err)

		if seekToKey {
			_, err = ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 11))
			assert.Nil(t, err)
		}
		return ssTable
	}

	var ssTables []*table.SSTable
	ssTables = append(ssTables, buildAnSSTable(1, true))
	ssTables = append(ssTables, buildAnSSTable(2, true))
	ssTables = append(ssTables, buildAnSSTable(3, false))

	ssTableCleaner.Start()
	ssTableCleaner.Submit(ssTables)
	time.Sleep(1 * time.Second)

	assert.Equal(t, 2, len(ssTableCleaner.PendingSSTablesToClean()))
	assert.Equal(t, uint64(1), ssTableCleaner.PendingSSTablesToClean()[0].Id())
	assert.Equal(t, uint64(2), ssTableCleaner.PendingSSTablesToClean()[1].Id())
}
