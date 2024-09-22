package compact

import (
	"go-lsm/compact/meta"
	"go-lsm/state"
)

type SimpleLeveledCompaction struct {
	options state.SimpleLeveledCompactionOptions
}

func NewSimpleLeveledCompaction(options state.SimpleLeveledCompactionOptions) SimpleLeveledCompaction {
	return SimpleLeveledCompaction{
		options: options,
	}
}

func (compaction SimpleLeveledCompaction) CompactionDescription(stateSnapshot state.StorageStateSnapshot) (meta.SimpleLeveledCompactionDescription, bool) {
	var ssTableCountByLevel []int
	ssTableCountByLevel = append(ssTableCountByLevel, len(stateSnapshot.L0SSTableIds))

	for _, level := range stateSnapshot.Levels {
		ssTableCountByLevel = append(ssTableCountByLevel, len(level.SSTableIds))
	}
	for level := 0; level < int(compaction.options.MaxLevels); level++ {
		if level == 0 {
			if ssTableCountByLevel[level] < int(compaction.options.Level0FilesCompactionTrigger) {
				continue
			}
		}
		lowerLevel := level + 1
		sizeRatioPercentage := (float64(ssTableCountByLevel[lowerLevel]) / float64(ssTableCountByLevel[level])) * 100
		if sizeRatioPercentage < float64(compaction.options.NumberOfSSTablesRatioPercentage) {
			println("Triggering simple leveled compaction between levels ", level, lowerLevel)
			var upperLevel int
			if level == 0 {
				upperLevel = -1
			} else {
				upperLevel = level
			}
			return meta.SimpleLeveledCompactionDescription{
				UpperLevel:           upperLevel,
				LowerLevel:           lowerLevel,
				UpperLevelSSTableIds: stateSnapshot.SSTableIdsAt(level),
				LowerLevelSSTableIds: stateSnapshot.SSTableIdsAt(lowerLevel),
			}, true
		}
	}
	return meta.NothingToCompactDescription, false
}
