package state

import (
	"go-lsm/compact/meta"
	"go-lsm/table"
	"go-lsm/table/block"
)

type StorageStateChangeEvent struct {
	NewSSTables   []table.SSTable
	NewSSTableIds []uint64
	description   meta.SimpleLeveledCompactionDescription
}

func NewStorageStateChangeEvent(newSSTables []table.SSTable, description meta.SimpleLeveledCompactionDescription) StorageStateChangeEvent {
	newSSTableIds := make([]uint64, 0, len(newSSTables))
	for _, ssTable := range newSSTables {
		newSSTableIds = append(newSSTableIds, ssTable.Id())
	}
	return StorageStateChangeEvent{
		NewSSTables:   newSSTables,
		NewSSTableIds: newSSTableIds,
		description:   description,
	}
}

func NewStorageStateChangeEventByOpeningSSTables(ssTableIds []uint64, description meta.SimpleLeveledCompactionDescription, rootPath string) (StorageStateChangeEvent, error) {
	newSSTables := make([]table.SSTable, 0, len(ssTableIds))
	for _, ssTableId := range ssTableIds {
		ssTable, err := table.Load(ssTableId, rootPath, block.DefaultBlockSize)
		if err != nil {
			return StorageStateChangeEvent{}, err
		}
		newSSTables = append(newSSTables, ssTable)
	}
	return StorageStateChangeEvent{
		NewSSTables:   newSSTables,
		NewSSTableIds: ssTableIds,
		description:   description,
	}, nil
}

func (event StorageStateChangeEvent) CompactionUpperLevel() int {
	return event.description.UpperLevel
}

func (event StorageStateChangeEvent) CompactionLowerLevel() int {
	return event.description.LowerLevel
}

func (event StorageStateChangeEvent) CompactionUpperLevelSSTableIds() []uint64 {
	return event.description.UpperLevelSSTableIds
}

func (event StorageStateChangeEvent) CompactionLowerLevelSSTableIds() []uint64 {
	return event.description.LowerLevelSSTableIds
}

func (event StorageStateChangeEvent) CompactionDescription() meta.SimpleLeveledCompactionDescription {
	return event.description
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
	ssTableIds := make(map[uint64]struct{}, len(event.CompactionUpperLevelSSTableIds()))
	for _, ssTableId := range event.CompactionUpperLevelSSTableIds() {
		ssTableIds[ssTableId] = struct{}{}
	}
	return ssTableIds
}
