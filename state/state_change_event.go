package state

import "go-lsm/table"

type StorageStateChangeEvent struct {
	NewSSTables          []table.SSTable
	NewSSTableIds        []uint64
	UpperLevel           int
	LowerLevel           int
	UpperLevelSSTableIds []uint64
	LowerLevelSSTableIds []uint64
}

func NewStorageStateChangeEvent(newSSTables []table.SSTable, upperLevel int, lowerLevel int, upperLevelSSTableIds []uint64, lowerLevelSSTableIds []uint64) StorageStateChangeEvent {
	newSSTableIds := make([]uint64, 0, len(newSSTables))
	for _, ssTable := range newSSTables {
		newSSTableIds = append(newSSTableIds, ssTable.Id())
	}
	return StorageStateChangeEvent{
		NewSSTables:          newSSTables,
		NewSSTableIds:        newSSTableIds,
		UpperLevel:           upperLevel,
		LowerLevel:           lowerLevel,
		UpperLevelSSTableIds: upperLevelSSTableIds,
		LowerLevelSSTableIds: lowerLevelSSTableIds,
	}
}

func (event StorageStateChangeEvent) allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds(ssTableIds []uint64) []uint64 {
	var excludedSSTableIds []uint64

	upperLevelSSTableIdsCompacted := event.upperLevelSSTableIdsAsMap()
	for _, ssTableId := range ssTableIds {
		if _, ok := upperLevelSSTableIdsCompacted[ssTableId]; !ok {
			excludedSSTableIds = append(excludedSSTableIds, ssTableId)
		}
	}
	return excludedSSTableIds
}

func (event StorageStateChangeEvent) upperLevelSSTableIdsAsMap() map[uint64]struct{} {
	ssTableIds := make(map[uint64]struct{}, len(event.UpperLevelSSTableIds))
	for _, ssTableId := range event.UpperLevelSSTableIds {
		ssTableIds[ssTableId] = struct{}{}
	}
	return ssTableIds
}
