package go_lsm

import (
	"go-lsm/iterator"
	"go-lsm/txn"
	"slices"
)

type StorageOptions struct {
	memTableSizeInBytes uint64
}

// StorageState TODO: Support concurrency
type StorageState struct {
	currentMemtable    *Memtable
	immutableMemtables []*Memtable
	idGenerator        *MemtableIdGenerator
	options            StorageOptions
}

func NewStorageState() *StorageState {
	return NewStorageStateWithOptions(StorageOptions{
		memTableSizeInBytes: 1 << 20,
	})
}

func NewStorageStateWithOptions(options StorageOptions) *StorageState {
	idGenerator := NewMemtableIdGenerator()
	return &StorageState{
		currentMemtable: NewMemtable(idGenerator.NextId()),
		idGenerator:     idGenerator,
		options:         options,
	}
}

func (storageState *StorageState) Get(key txn.Key) (txn.Value, bool) {
	value, ok := storageState.currentMemtable.Get(key)
	if ok {
		return value, ok
	}
	for index := len(storageState.immutableMemtables) - 1; index >= 0; index-- {
		memTable := storageState.immutableMemtables[index]
		if value, ok := memTable.Get(key); ok {
			return value, ok
		}
	}
	return txn.EmptyValue, false
}

func (storageState *StorageState) Set(batch *txn.Batch) {
	for _, entry := range batch.AllEntries() {
		if entry.IsKindPut() {
			storageState.currentMemtable.Set(entry.Key, entry.Value)
		} else if entry.IsKindDelete() {
			storageState.currentMemtable.Delete(entry.Key)
		} else {
			panic("Unsupported entry type")
		}
		storageState.mayBeFreezeCurrentMemtable()
	}
}

func (storageState *StorageState) Scan(inclusiveRange txn.InclusiveKeyRange) iterator.Iterator {
	iterators := make([]iterator.Iterator, len(storageState.immutableMemtables)+1)

	index := 0
	iterators[index] = storageState.currentMemtable.Scan(inclusiveRange)

	index += 1
	for immutableMemtableIndex := len(storageState.immutableMemtables) - 1; immutableMemtableIndex >= 0; immutableMemtableIndex-- {
		iterators[index] = storageState.immutableMemtables[immutableMemtableIndex].Scan(inclusiveRange)
		index += 1
	}
	return iterator.NewMergeIterator(iterators)
}

func (storageState *StorageState) hasImmutableMemtables() bool {
	return len(storageState.immutableMemtables) > 0
}

func (storageState *StorageState) sortedMemtableIds() []uint64 {
	ids := make([]uint64, 0, 1+len(storageState.immutableMemtables))
	ids = append(ids, storageState.currentMemtable.id)
	for _, immutableMemtable := range storageState.immutableMemtables {
		ids = append(ids, immutableMemtable.id)
	}
	slices.Sort(ids)
	return ids
}

// TODO: Generate new id
// TODO: Manifest
// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable() {
	if storageState.currentMemtable.Size() >= storageState.options.memTableSizeInBytes {
		storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
		storageState.currentMemtable = NewMemtable(storageState.idGenerator.NextId())
	}
}

// forceFreezeCurrentMemtable only for testing.
func (storageState *StorageState) forceFreezeCurrentMemtable() {
	storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
	storageState.currentMemtable = NewMemtable(storageState.idGenerator.NextId())
}
