package state

import "go-lsm/table"

type StorageStateChangeEvent struct {
	newSSTables          []table.SSTable
	newSSTableIds        []uint64
	upperLevel           int
	lowerLevel           int
	upperLevelSSTableIds []uint64
	lowerLevelSSTableIds []uint64
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
	ssTableIds := make(map[uint64]struct{}, len(event.upperLevelSSTableIds))
	for _, ssTableId := range event.upperLevelSSTableIds {
		ssTableIds[ssTableId] = struct{}{}
	}
	return ssTableIds
}
