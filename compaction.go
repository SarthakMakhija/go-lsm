package go_lsm

import (
	"go-lsm/iterator"
	"go-lsm/table"
	"os"
	"slices"
)

// ForceFullCompaction
// TODO: concurrency
// TODO: Sync directory & Manifest
func (storageState *StorageState) ForceFullCompaction() error {
	l0SSTableIds := storageState.orderedSSTableIds(level0)
	l1SSTableIds := storageState.orderedSSTableIds(level1)
	combinedSSTableIds := append(l0SSTableIds, l1SSTableIds...)

	runCompaction := func() ([]table.SSTable, error) {
		newSSTables, err := storageState.runFullCompaction(
			l0SSTableIds,
			l1SSTableIds,
		)
		if err != nil {
			return nil, err
		}
		return newSSTables, nil
	}
	updateSSTableMappingState := func(newSSTables []table.SSTable) []uint64 {
		for _, ssTableId := range combinedSSTableIds {
			delete(storageState.ssTables, ssTableId)
		}
		newSSTableIds := make([]uint64, 0, len(newSSTables))
		for _, ssTable := range newSSTables {
			storageState.ssTables[ssTable.Id()] = ssTable
			newSSTableIds = append(newSSTableIds, ssTable.Id())
		}
		return newSSTableIds
	}
	updateLevel1State := func(ids []uint64) {
		storageState.levels[level1-1] = &Level{levelNumber: 1, ssTableIds: ids}
	}
	updateL0State := func(ids []uint64) {
		var updatedL0SSTableIds []uint64
		for _, currentL0SSTableId := range storageState.l0SSTableIds {
			if !slices.Contains(l0SSTableIds, currentL0SSTableId) {
				updatedL0SSTableIds = append(updatedL0SSTableIds, currentL0SSTableId)
			}
		}
		storageState.l0SSTableIds = updatedL0SSTableIds
	}
	removeSSTableFiles := func() {
		for _, ssTableId := range combinedSSTableIds {
			_ = os.Remove(storageState.ssTableFilePath(ssTableId))
		}
	}

	ssTables, err := runCompaction()
	if err != nil {
		return err
	}
	newSSTableIds := updateSSTableMappingState(ssTables)
	updateLevel1State(newSSTableIds)
	updateL0State(newSSTableIds)
	removeSSTableFiles()
	return nil
}

func (storageState *StorageState) runFullCompaction(l0SSTableIds []uint64, l1SSTableIds []uint64) ([]table.SSTable, error) {
	ssTableIds := append(l0SSTableIds, l1SSTableIds...)
	iterators := make([]iterator.Iterator, 0, len(l0SSTableIds)+len(l1SSTableIds))

	for _, id := range ssTableIds {
		ssTableIterator, err := storageState.ssTables[id].SeekToFirst()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, ssTableIterator)
	}
	return storageState.ssTablesFromIterator(iterator.NewMergeIterator(iterators)) //TODO: Maybe an iterator which does not require seek?
}

func (storageState *StorageState) ssTablesFromIterator(iterator iterator.Iterator) ([]table.SSTable, error) {
	var ssTableBuilder *table.SSTableBuilder
	var newSSTables []table.SSTable

	for iterator.IsValid() {
		if ssTableBuilder == nil {
			ssTableBuilder = table.NewSSTableBuilderWithDefaultBlockSize()
		}
		if !iterator.Value().IsEmpty() {
			ssTableBuilder.Add(iterator.Key(), iterator.Value())
		}
		if err := iterator.Next(); err != nil {
			return nil, err
		}
		if int64(ssTableBuilder.EstimatedSize()) >= storageState.options.SSTableSizeInBytes {
			ssTable, err := storageState.buildNewSStable(ssTableBuilder)
			if err != nil {
				return nil, err
			}
			newSSTables = append(newSSTables, ssTable)
			ssTableBuilder = nil
		}
	}
	if ssTableBuilder != nil {
		ssTable, err := storageState.buildNewSStable(ssTableBuilder)
		if err != nil {
			return nil, err
		}
		newSSTables = append(newSSTables, ssTable)
	}
	return newSSTables, nil
}

func (storageState *StorageState) buildNewSStable(ssTableBuilder *table.SSTableBuilder) (table.SSTable, error) {
	ssTableId := storageState.idGenerator.NextId()
	ssTable, err := ssTableBuilder.Build(ssTableId, storageState.ssTableFilePath(ssTableId))
	if err != nil {
		return table.SSTable{}, err
	}
	return ssTable, nil
}
