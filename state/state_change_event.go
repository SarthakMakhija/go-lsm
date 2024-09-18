package state

import (
	"go-lsm/compact/meta"
	"go-lsm/table"
)

type StorageStateChangeEvent struct {
	NewSSTables   []table.SSTable
	NewSSTableIds []uint64
	Description   meta.SimpleLeveledCompactionDescription
}

func NewStorageStateChangeEvent(newSSTables []table.SSTable, description meta.SimpleLeveledCompactionDescription) StorageStateChangeEvent {
	newSSTableIds := make([]uint64, 0, len(newSSTables))
	for _, ssTable := range newSSTables {
		newSSTableIds = append(newSSTableIds, ssTable.Id())
	}
	return StorageStateChangeEvent{
		NewSSTables:   newSSTables,
		NewSSTableIds: newSSTableIds,
		Description:   description,
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
	ssTableIds := make(map[uint64]struct{}, len(event.Description.UpperLevelSSTableIds))
	for _, ssTableId := range event.Description.UpperLevelSSTableIds {
		ssTableIds[ssTableId] = struct{}{}
	}
	return ssTableIds
}
