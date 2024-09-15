package compact

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/state"
	"testing"
)

func TestGenerateCompactionTaskForSimpleLayeredCompactionWithNoCompaction(t *testing.T) {
	compactionOptions := state.SimpleLeveledCompactionOptions{
		SizeRatioPercentage:          200,
		MaxLevels:                    2,
		Level0FilesCompactionTrigger: 2,
	}
	snapshot := state.StorageStateSnapshot{
		L0SSTableIds: []uint64{1},
		Levels: []*state.Level{
			{LevelNumber: 1, SSTableIds: []uint64{2, 3}},
			{LevelNumber: 2, SSTableIds: []uint64{4, 5, 6, 7}},
		},
	}

	compaction := NewSimpleLeveledCompaction(compactionOptions)
	_, ok := compaction.CompactionDescription(snapshot)

	assert.False(t, ok)
}

func TestGenerateCompactionTaskForSimpleLayeredCompactionWithCompactionForLevel0And1(t *testing.T) {
	compactionOptions := state.SimpleLeveledCompactionOptions{
		SizeRatioPercentage:          200,
		MaxLevels:                    2,
		Level0FilesCompactionTrigger: 2,
	}
	snapshot := state.StorageStateSnapshot{
		L0SSTableIds: []uint64{1, 2},
		Levels: []*state.Level{
			{LevelNumber: 1, SSTableIds: nil},
		},
	}

	compaction := NewSimpleLeveledCompaction(compactionOptions)
	compactionDescription, ok := compaction.CompactionDescription(snapshot)

	assert.True(t, ok)
	assert.Equal(t, -1, compactionDescription.upperLevel)
	assert.Equal(t, 1, compactionDescription.lowerLevel)
	assert.Equal(t, []uint64{2, 1}, compactionDescription.upperLevelSSTableIds)
	assert.Equal(t, []uint64{}, compactionDescription.lowerLevelSSTableIds)
}

func TestGenerateCompactionTaskForSimpleLayeredCompactionWithCompactionForLevel1And2(t *testing.T) {
	compactionOptions := state.SimpleLeveledCompactionOptions{
		SizeRatioPercentage:          200,
		MaxLevels:                    2,
		Level0FilesCompactionTrigger: 2,
	}
	snapshot := state.StorageStateSnapshot{
		L0SSTableIds: []uint64{1},
		Levels: []*state.Level{
			{LevelNumber: 1, SSTableIds: []uint64{2, 3}},
			{LevelNumber: 2, SSTableIds: []uint64{4}},
		},
	}

	compaction := NewSimpleLeveledCompaction(compactionOptions)
	compactionDescription, ok := compaction.CompactionDescription(snapshot)

	assert.True(t, ok)
	assert.Equal(t, 1, compactionDescription.upperLevel)
	assert.Equal(t, 2, compactionDescription.lowerLevel)
	assert.Equal(t, []uint64{3, 2}, compactionDescription.upperLevelSSTableIds)
	assert.Equal(t, []uint64{4}, compactionDescription.lowerLevelSSTableIds)
}
