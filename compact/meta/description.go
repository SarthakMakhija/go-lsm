package meta

// NothingToCompactDescription represents none compaction.
var NothingToCompactDescription = SimpleLeveledCompactionDescription{}

// SimpleLeveledCompactionDescription defines the table.SSTable ids between adjacent levels which will undergo compaction.
type SimpleLeveledCompactionDescription struct {
	UpperLevel           int
	LowerLevel           int
	UpperLevelSSTableIds []uint64
	LowerLevelSSTableIds []uint64
}
