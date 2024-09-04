package state

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/table"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateCompactionTaskForSimpleLayeredCompaction(t *testing.T) {
	tempDirectory := os.TempDir()

	simpleLeveledCompactionOptions := SimpleLeveledCompactionOptions{
		sizeRatioPercentage:          200,
		maxLevels:                    totalLevels,
		level0FilesCompactionTrigger: 2,
	}
	storageOptions := StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  tempDirectory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
		compactionOptions:     simpleLeveledCompactionOptions,
	}
	storageState, _ := NewStorageStateWithOptions(storageOptions, true)
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
	}()

	buildL0SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("paxos"))

		filePath := filepath.Join(tempDirectory, fmt.Sprintf("TestGenerateCompactionTaskForSimpleLayeredCompaction%v.log", id))

		ssTable, err := ssTableBuilder.Build(id, filePath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable
	}
	buildL1SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("unique", 30), kv.NewStringValue("map"))

		filePath := filepath.Join(tempDirectory, fmt.Sprintf("TestGenerateCompactionTaskForSimpleLayeredCompaction%v.log", id))

		ssTable, err := ssTableBuilder.Build(id, filePath)
		assert.Nil(t, err)

		level := storageState.levels[level1-1]
		if level == nil {
			level = &Level{levelNumber: 1}
		}
		level.ssTableIds = append(level.ssTableIds, id)
		storageState.levels[level1-1] = level
		storageState.ssTables[id] = ssTable
	}

	buildL0SSTable(storageState.idGenerator.NextId())
	buildL0SSTable(storageState.idGenerator.NextId())
	buildL1SSTable(storageState.idGenerator.NextId())

	assert.Equal(t, []uint64{3, 2}, storageState.orderedSSTableIds(level0)) //id 1 is for current memtable
	assert.Equal(t, []uint64{4}, storageState.orderedSSTableIds(level1))

	controller := NewSimpleLeveledCompactionController(simpleLeveledCompactionOptions)
	compactionTask := controller.GenerateCompactionTask(storageState)

	assert.Equal(t, 1, compactionTask.lowerLevel)
	assert.Equal(t, -1, compactionTask.upperLevel)
	assert.Equal(t, []uint64{4}, compactionTask.lowerLevelSSTableIds)
	assert.Equal(t, []uint64{3, 2}, compactionTask.upperLevelSSTableIds)
}
