package state

import (
	"go-lsm/compact/meta"
	"go-lsm/table"
	"go-lsm/table/block"
	"slices"
)

var NoStorageStateChanges = StorageStateChangeEvent{anyChanges: false}

// StorageStateChangeEvent represents a state change event for StorageState.
// It is generated after compaction runs, and it compacts table.SSTable files from adjacent levels.
type StorageStateChangeEvent struct {
	NewSSTables   []*table.SSTable
	NewSSTableIds []uint64
	description   meta.SimpleLeveledCompactionDescription
	anyChanges    bool
}

// NewStorageStateChangeEvent creates a new instance of StorageStateChangeEvent.
func NewStorageStateChangeEvent(newSSTables []*table.SSTable, description meta.SimpleLeveledCompactionDescription) StorageStateChangeEvent {
	newSSTableIds := make([]uint64, 0, len(newSSTables))
	for _, ssTable := range newSSTables {
		newSSTableIds = append(newSSTableIds, ssTable.Id())
	}
	return StorageStateChangeEvent{
		NewSSTables:   newSSTables,
		NewSSTableIds: newSSTableIds,
		description:   description,
		anyChanges:    true,
	}
}

// NewStorageStateChangeEventByOpeningSSTables creates a new instance of StorageStateChangeEvent, by opening the newSSTableIds.
func NewStorageStateChangeEventByOpeningSSTables(newSSTableIds []uint64, description meta.SimpleLeveledCompactionDescription, rootPath string) (StorageStateChangeEvent, error) {
	newSSTables := make([]*table.SSTable, 0, len(newSSTableIds))
	for _, ssTableId := range newSSTableIds {
		ssTable, err := table.Load(ssTableId, rootPath, block.DefaultBlockSize)
		if err != nil {
			return NoStorageStateChanges, err
		}
		newSSTables = append(newSSTables, ssTable)
	}
	return StorageStateChangeEvent{
		NewSSTables:   newSSTables,
		NewSSTableIds: newSSTableIds,
		description:   description,
		anyChanges:    true,
	}, nil
}

// CompactionUpperLevel returns the upper level present in meta.SimpleLeveledCompactionDescription.
func (event StorageStateChangeEvent) CompactionUpperLevel() int {
	return event.description.UpperLevel
}

// CompactionLowerLevel returns the lower level present in meta.SimpleLeveledCompactionDescription.
func (event StorageStateChangeEvent) CompactionLowerLevel() int {
	return event.description.LowerLevel
}

// CompactionUpperLevelSSTableIds returns the SSTableIds present in upper level of meta.SimpleLeveledCompactionDescription.
func (event StorageStateChangeEvent) CompactionUpperLevelSSTableIds() []uint64 {
	return event.description.UpperLevelSSTableIds
}

// CompactionLowerLevelSSTableIds returns the SSTableIds present in lower level of meta.SimpleLeveledCompactionDescription.
func (event StorageStateChangeEvent) CompactionLowerLevelSSTableIds() []uint64 {
	return event.description.LowerLevelSSTableIds
}

// CompactionDescription returns the instance of meta.SimpleLeveledCompactionDescription.
func (event StorageStateChangeEvent) CompactionDescription() meta.SimpleLeveledCompactionDescription {
	return event.description
}

// MaxSSTableId returns the max SSTableId from NewSSTableIds.
func (event StorageStateChangeEvent) MaxSSTableId() uint64 {
	return slices.Max(event.NewSSTableIds)
}

// HasAnyChanges returns true if StorageStateChangeEvent has any changes, meaning if the compaction ran between two levels.
func (event StorageStateChangeEvent) HasAnyChanges() bool {
	return event.anyChanges
}

// allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds returns all the SSTableIds from the upper level, excluding the
// given SSTableIds.
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

// upperLevelSSTableIdsAsMap returns the SSTableIds present in upper level as a map.
func (event StorageStateChangeEvent) upperLevelSSTableIdsAsMap() map[uint64]struct{} {
	ssTableIds := make(map[uint64]struct{}, len(event.CompactionUpperLevelSSTableIds()))
	for _, ssTableId := range event.CompactionUpperLevelSSTableIds() {
		ssTableIds[ssTableId] = struct{}{}
	}
	return ssTableIds
}
