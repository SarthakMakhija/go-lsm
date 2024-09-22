package meta

// NothingToCompactDescription represents none compaction.
var NothingToCompactDescription = SimpleLeveledCompactionDescription{}

// SimpleLeveledCompactionDescription defines the table.SSTable ids between adjacent levels which will undergo compaction.
// Between level0 and level1, level0 would be UpperLevel, level1 would be LowerLevel.
// Similarly, between level1 and level2, level1 would be UpperLevel, level2 would be LowerLevel.
type SimpleLeveledCompactionDescription struct {
	UpperLevel           int
	LowerLevel           int
	UpperLevelSSTableIds []uint64
	LowerLevelSSTableIds []uint64
}
