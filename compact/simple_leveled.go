package compact

import "go-lsm/state"

type SimpleLeveledCompactionDescription struct {
	upperLevel           int
	lowerLevel           int
	upperLevelSSTableIds []uint64
	lowerLevelSSTableIds []uint64
}

type SimpleLeveledCompaction struct {
	options state.SimpleLeveledCompactionOptions
}

func NewSimpleLeveledCompaction(options state.SimpleLeveledCompactionOptions) SimpleLeveledCompaction {
	return SimpleLeveledCompaction{
		options: options,
	}
}

func (compaction SimpleLeveledCompaction) CompactionDescription(stateSnapshot state.StorageStateSnapshot) (SimpleLeveledCompactionDescription, bool) {
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
		if sizeRatioPercentage < float64(compaction.options.SizeRatioPercentage) {
			println("Triggering simple leveled compaction between levels ", level, lowerLevel)
			var upperLevel int
			if level == 0 {
				upperLevel = -1
			} else {
				upperLevel = level
			}
			return SimpleLeveledCompactionDescription{
				upperLevel:           upperLevel,
				lowerLevel:           lowerLevel,
				upperLevelSSTableIds: stateSnapshot.OrderedSSTableIds(level),
				lowerLevelSSTableIds: stateSnapshot.OrderedSSTableIds(lowerLevel),
			}, true
		}
	}
	return SimpleLeveledCompactionDescription{}, false
}
