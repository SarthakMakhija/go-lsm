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

func TestForceFullCompaction(t *testing.T) {
	tempDirectory := os.TempDir()

	storageOptions := StorageOptions{
		MemTableSizeInBytes:   250,
		Path:                  tempDirectory,
		MaximumMemtables:      2,
		FlushMemtableDuration: 1 * time.Millisecond,
		SSTableSizeInBytes:    4096,
		compactionOptions: SimpleLeveledCompactionOptions{
			sizeRatioPercentage:          200,
			maxLevels:                    totalLevels,
			level0FilesCompactionTrigger: 2,
		},
	}
	storageState, _ := NewStorageStateWithOptions(storageOptions)
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
	}()

	buildL0SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("consensus", 6), kv.NewStringValue("paxos"))
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("TiKV"))
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("etcd", 8), kv.NewStringValue("bbolt"))

		filePath := filepath.Join(tempDirectory, fmt.Sprintf("TestForceFullCompaction%v.log", id))

		ssTable, err := ssTableBuilder.Build(id, filePath)
		assert.Nil(t, err)

		storageState.l0SSTableIds = append(storageState.l0SSTableIds, id)
		storageState.ssTables[id] = ssTable
	}
	buildL1SSTable := func(id uint64) {
		ssTableBuilder := table.NewSSTableBuilder(4096)
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("bolt", 9), kv.NewStringValue("b+tree"))
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("quorum", 10), kv.NewStringValue("n/2+1"))
		ssTableBuilder.Add(kv.NewStringKeyWithTimestamp("unique", 11), kv.NewStringValue("map"))

		filePath := filepath.Join(tempDirectory, fmt.Sprintf("TestForceFullCompaction%v.log", id))

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
	buildL1SSTable(storageState.idGenerator.NextId())

	assert.Equal(t, []uint64{2}, storageState.orderedSSTableIds(level0)) //id 1 is for current memtable
	assert.Equal(t, []uint64{3}, storageState.orderedSSTableIds(level1))

	assert.Nil(t, storageState.ForceFullCompaction())
	level1SSTableIds := storageState.orderedSSTableIds(level1)

	assert.Equal(t, 1, len(level1SSTableIds))
	level1SSTableId := level1SSTableIds[0]

	iterator, err := storageState.ssTables[level1SSTableId].SeekToFirst()
	assert.Nil(t, err)

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("bolt", 9), iterator.Key())
	assert.Equal(t, kv.NewStringValue("b+tree"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 6), iterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 7), iterator.Key())
	assert.Equal(t, kv.NewStringValue("TiKV"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("etcd", 8), iterator.Key())
	assert.Equal(t, kv.NewStringValue("bbolt"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("quorum", 10), iterator.Key())
	assert.Equal(t, kv.NewStringValue("n/2+1"), iterator.Value())

	_ = iterator.Next()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("unique", 11), iterator.Key())
	assert.Equal(t, kv.NewStringValue("map"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
