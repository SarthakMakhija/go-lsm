package meta

var NothingToCompactDescription = SimpleLeveledCompactionDescription{}

type SimpleLeveledCompactionDescription struct {
	UpperLevel           int
	LowerLevel           int
	UpperLevelSSTableIds []uint64
	LowerLevelSSTableIds []uint64
}
