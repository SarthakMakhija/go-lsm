package compact

import (
	"go-lsm/compact/meta"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/table"
	"go-lsm/txn"
)

// Compaction represents core logic to compact table.SSTable files.
type Compaction struct {
	oracle      *txn.Oracle
	idGenerator *state.SSTableIdGenerator
	options     state.StorageOptions
}

// NewCompaction creates a new instance of Compaction.
func NewCompaction(oracle *txn.Oracle, idGenerator *state.SSTableIdGenerator, options state.StorageOptions) *Compaction {
	return &Compaction{
		oracle:      oracle,
		idGenerator: idGenerator,
		options:     options,
	}
}

// Start performs compaction given an instance of state.StorageStateSnapshot.
// It is called from compaction goroutine at fixed intervals.
// It returns an instance of state.StorageStateChangeEvent if any two levels are eligible for compaction.
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

// compact performs compaction by creating an instance of iterator.MergeIterator using the iterators present in adjacent levels
// defined in meta.SimpleLeveledCompactionDescription.
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

// ssTablesFromIterator creates a slice of table.SSTable (/new SSTables) from the given iterator.
// It skips all the keys with commit-timestamp <= maximum read-timestamp.
// If the maximum read-timestamp in the system is 9, there is no point in storing any key with commit-timestamp < 9,
// because all the read operations will be getting read-timestamp > 9 from txn.Oracle.
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

// buildNewSStable creates a new instance of table.SSTable.
func (compaction *Compaction) buildNewSStable(ssTableBuilder *table.SSTableBuilder) (*table.SSTable, error) {
	ssTableId := compaction.idGenerator.NextId()
	ssTable, err := ssTableBuilder.Build(ssTableId, compaction.options.Path)
	if err != nil {
		return nil, err
	}
	return ssTable, nil
}
