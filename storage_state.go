package go_lsm

import (
	"go-lsm/iterator"
	"go-lsm/txn"
)

type StorageOptions struct {
	memTableSizeInBytes uint64
}

// StorageState TODO: Support concurrency
type StorageState struct {
	currentMemTable    *MemTable
	immutableMemTables []*MemTable
	options            StorageOptions
}

func NewStorageState() *StorageState {
	return &StorageState{
		currentMemTable: NewMemTable(1),
		options: StorageOptions{
			memTableSizeInBytes: 1 << 20,
		},
	}
}

func NewStorageStateWithOptions(options StorageOptions) *StorageState {
	return &StorageState{
		currentMemTable: NewMemTable(1),
		options:         options,
	}
}

func (storageState *StorageState) Get(key txn.Key) (txn.Value, bool) {
	value, ok := storageState.currentMemTable.Get(key)
	if ok {
		return value, ok
	}
	for index := len(storageState.immutableMemTables) - 1; index >= 0; index-- {
		memTable := storageState.immutableMemTables[index]
		if value, ok := memTable.Get(key); ok {
			return value, ok
		}
	}
	return txn.EmptyValue, false
}

func (storageState *StorageState) Set(batch *txn.Batch) {
	for _, entry := range batch.AllEntries() {
		if entry.IsKindPut() {
			storageState.currentMemTable.Set(entry.Key, entry.Value)
		} else if entry.IsKindDelete() {
			storageState.currentMemTable.Delete(entry.Key)
		} else {
			panic("Unsupported entry type")
		}
		storageState.mayBeFreezeCurrentMemtable()
	}
}

func (storageState *StorageState) Scan(inclusiveRange txn.InclusiveRange) iterator.Iterator {
	iterators := make([]iterator.Iterator, len(storageState.immutableMemTables)+1)

	index := 0
	iterators[index] = storageState.currentMemTable.Scan(inclusiveRange)

	index += 1
	for immutableMemTableIndex := len(storageState.immutableMemTables) - 1; immutableMemTableIndex >= 0; immutableMemTableIndex-- {
		iterators[index] = storageState.immutableMemTables[immutableMemTableIndex].Scan(inclusiveRange)
		index += 1
	}
	return iterator.NewMergeIterator(iterators)
}

func (storageState *StorageState) hasImmutableMemTables() bool {
	return len(storageState.immutableMemTables) > 0
}

// TODO: Generate new id
// TODO: Manifest
// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable() {
	if storageState.currentMemTable.Size() >= storageState.options.memTableSizeInBytes {
		storageState.immutableMemTables = append(storageState.immutableMemTables, storageState.currentMemTable)
		storageState.currentMemTable = NewMemTable(1)
	}
}

// forceFreezeCurrentMemtable only for testing.
func (storageState *StorageState) forceFreezeCurrentMemtable() {
	storageState.immutableMemTables = append(storageState.immutableMemTables, storageState.currentMemTable)
	storageState.currentMemTable = NewMemTable(1)
}
