package go_lsm

type StorageOptions struct {
	memTableSizeInBytes uint64
}

// StorageState TODO: Support concurrency
type StorageState struct {
	currentMemTable    *MemTable
	immutableMemtables []*MemTable
	options            StorageOptions
}

func NewStorageState() *StorageState {
	return &StorageState{
		currentMemTable: NewMemtable(1),
		options: StorageOptions{
			memTableSizeInBytes: 1 << 20,
		},
	}
}

func NewStorageStateWithOptions(options StorageOptions) *StorageState {
	return &StorageState{
		currentMemTable: NewMemtable(1),
		options:         options,
	}
}

func (storageState *StorageState) Get(key Key) (Value, bool) {
	value, ok := storageState.currentMemTable.Get(key)
	if ok {
		return value, ok
	}
	for index := len(storageState.immutableMemtables) - 1; index >= 0; index-- {
		memTable := storageState.immutableMemtables[index]
		if value, ok := memTable.Get(key); ok {
			return value, ok
		}
	}
	return emptyValue, false
}

func (storageState *StorageState) Set(batch *Batch) {
	for _, entry := range batch.entries {
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

func (storageState *StorageState) hasImmutableMemTables() bool {
	return len(storageState.immutableMemtables) > 0
}

// TODO: Generate new id
// TODO: Manifest
// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable() {
	if storageState.currentMemTable.Size() >= storageState.options.memTableSizeInBytes {
		memtable := NewMemtable(1)
		storageState.immutableMemtables = append(storageState.immutableMemtables, memtable)
		storageState.currentMemTable = memtable
	}
}
