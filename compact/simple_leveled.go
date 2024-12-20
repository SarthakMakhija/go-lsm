package compact

import (
	"go-lsm/compact/meta"
	"go-lsm/state"
)

// SimpleLeveledCompaction represents a leveled compaction strategy which compacts all the table.SSTable files between two levels.
// It considers two options for deciding if compaction needs to run.
// Option1: Level0FilesCompactionTrigger.
// This defines the number of table.SSTable files at level0 which should trigger compaction.
// Consider Level0FilesCompactionTrigger = 2, and number of table.SSTable files at level0 = 3.
// This means all table.SSTable files present at level0 are eligible for undergoing compaction with all the table.SSTable files at
// level1.
// Option2: NumberOfSSTablesRatioPercentage.
// This defines the ratio between the number of table.SSTable files present in two adjacent levels:
// number of files at lower level / number of files at upper level.
// Consider NumberOfSSTablesRatioPercentage = 200, and number of table.SSTable files at level1 = 2, and at level2 = 1.
// Ratio = (1/2)*100 = 50%.
// This is less than the configured NumberOfSSTablesRatioPercentage. Hence, table.SSTable files will undergo compaction between
// level1 and level2.  This typically means that the number of files in lower level(s) should be more than the number of files in upper level(s).
// In the actual SimpleLeveledCompaction, we consider the file count instead of file size.
type SimpleLeveledCompaction struct {
	options state.SimpleLeveledCompactionOptions
}

// NewSimpleLeveledCompaction creates a new instance of SimpleLeveledCompaction.
func NewSimpleLeveledCompaction(options state.SimpleLeveledCompactionOptions) SimpleLeveledCompaction {
	return SimpleLeveledCompaction{
		options: options,
	}
}

// CompactionDescription returns the meta.SimpleLeveledCompactionDescription.
// If UpperLevel in meta.SimpleLeveledCompactionDescription is -1, it denotes, level0.
// It returns an instance of meta.SimpleLeveledCompactionDescription if any two levels are eligible for compaction, else
// it returns meta.NothingToCompactDescription, false.
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
		countRatioPercentage := (float64(ssTableCountByLevel[lowerLevel]) / float64(ssTableCountByLevel[level])) * 100
		if countRatioPercentage < float64(compaction.options.NumberOfSSTablesRatioPercentage) {
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
