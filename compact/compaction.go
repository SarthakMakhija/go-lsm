package compact

import (
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

func (compaction *Compaction) ssTablesFromIterator(iterator iterator.Iterator) ([]table.SSTable, error) {
	var ssTableBuilder *table.SSTableBuilder
	var newSSTables []table.SSTable

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

func (compaction *Compaction) buildNewSStable(ssTableBuilder *table.SSTableBuilder) (table.SSTable, error) {
	ssTableId := compaction.idGenerator.NextId()
	ssTable, err := ssTableBuilder.Build(ssTableId, compaction.options.Path)
	if err != nil {
		return table.SSTable{}, err
	}
	return ssTable, nil
}
