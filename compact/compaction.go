package compact

import (
	"go-lsm/compact/meta"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/table"
	"go-lsm/txn"
)

type Compaction struct {
	oracle      *txn.Oracle
	idGenerator *state.SSTableIdGenerator
	options     state.StorageOptions
}

func NewCompaction(oracle *txn.Oracle, idGenerator *state.SSTableIdGenerator, options state.StorageOptions) *Compaction {
	return &Compaction{
		oracle:      oracle,
		idGenerator: idGenerator,
		options:     options,
	}
}

func (compaction *Compaction) Start(snapshot state.StorageStateSnapshot) (state.StorageStateChangeEvent, error) {
	simpleLeveledCompaction := NewSimpleLeveledCompaction(compaction.options.CompactionOptions.StrategyOptions)
	description, ok := simpleLeveledCompaction.CompactionDescription(snapshot)
	if !ok {
		return state.NoStorageStateChanges, nil
	}
	ssTables, err := compaction.compact(description, snapshot)
	if err != nil {
		return state.NoStorageStateChanges, nil
	}
	event := state.NewStorageStateChangeEvent(ssTables, description)
	return event, nil
}

func (compaction *Compaction) compact(description meta.SimpleLeveledCompactionDescription, snapshot state.StorageStateSnapshot) ([]*table.SSTable, error) {
	upperLevelSSTableIterator := make([]iterator.Iterator, 0, len(description.UpperLevelSSTableIds))
	for _, ssTableId := range description.UpperLevelSSTableIds {
		ssTable := snapshot.SSTables[ssTableId]
		ssTableIterator, err := ssTable.SeekToFirst()
		if err != nil {
			return nil, nil
		}
		upperLevelSSTableIterator = append(upperLevelSSTableIterator, ssTableIterator)
	}
	lowerLevelSSTableIterator := make([]iterator.Iterator, 0, len(description.LowerLevelSSTableIds))
	for _, ssTableId := range description.LowerLevelSSTableIds {
		ssTable := snapshot.SSTables[ssTableId]
		ssTableIterator, err := ssTable.SeekToFirst()
		if err != nil {
			return nil, nil
		}
		lowerLevelSSTableIterator = append(lowerLevelSSTableIterator, ssTableIterator)
	}
	var iterators []iterator.Iterator
	iterators = append(upperLevelSSTableIterator, lowerLevelSSTableIterator...)
	return compaction.ssTablesFromIterator(iterator.NewMergeIterator(iterators, iterator.NoOperationOnCloseCallback))
}

func (compaction *Compaction) ssTablesFromIterator(iterator iterator.Iterator) ([]*table.SSTable, error) {
	var ssTableBuilder *table.SSTableBuilder
	var newSSTables []*table.SSTable

	var lastKey = kv.EmptyKey
	var firstKeyOccurrence = false
	var maxBeginTimestamp = compaction.oracle.MaxBeginTimestamp()

	for iterator.IsValid() {
		if ssTableBuilder == nil {
			ssTableBuilder = table.NewSSTableBuilderWithDefaultBlockSize()
		}
		sameAsLastRawKey := iterator.Key().IsRawKeyEqualTo(lastKey)
		if !sameAsLastRawKey {
			firstKeyOccurrence = true
		}

		if !sameAsLastRawKey && iterator.Key().Timestamp() <= maxBeginTimestamp && iterator.Value().IsEmpty() {
			lastKey = iterator.Key()
			if err := iterator.Next(); err != nil {
				return nil, err
			}
			continue
		}
		if iterator.Key().Timestamp() <= maxBeginTimestamp {
			if sameAsLastRawKey && !firstKeyOccurrence {
				if err := iterator.Next(); err != nil {
					return nil, err
				}
				continue
			}
			firstKeyOccurrence = false
		}
		if int64(ssTableBuilder.EstimatedSize()) >= compaction.options.SSTableSizeInBytes && !sameAsLastRawKey {
			ssTable, err := compaction.buildNewSStable(ssTableBuilder)
			if err != nil {
				return nil, err
			}
			newSSTables = append(newSSTables, ssTable)
			ssTableBuilder = table.NewSSTableBuilderWithDefaultBlockSize()
		}
		ssTableBuilder.Add(iterator.Key(), iterator.Value())
		if !sameAsLastRawKey {
			lastKey = iterator.Key()
		}
		if err := iterator.Next(); err != nil {
			return nil, err
		}
	}
	if ssTableBuilder != nil {
		ssTable, err := compaction.buildNewSStable(ssTableBuilder)
		if err != nil {
			return nil, err
		}
		newSSTables = append(newSSTables, ssTable)
	}
	return newSSTables, nil
}

func (compaction *Compaction) buildNewSStable(ssTableBuilder *table.SSTableBuilder) (*table.SSTable, error) {
	ssTableId := compaction.idGenerator.NextId()
	ssTable, err := ssTableBuilder.Build(ssTableId, compaction.options.Path)
	if err != nil {
		return nil, err
	}
	return ssTable, nil
}
