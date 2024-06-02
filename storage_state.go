package go_lsm

import (
	"fmt"
	"go-lsm/iterator"
	"go-lsm/table"
	"go-lsm/txn"
	"os"
	"path/filepath"
	"slices"
	"time"
)

type StorageOptions struct {
	MemTableSizeInBytes   uint64
	Path                  string
	MaximumMemtables      uint
	FlushMemtableDuration time.Duration
}

// StorageState TODO: Support concurrency and Close method
type StorageState struct {
	currentMemtable                *Memtable
	immutableMemtables             []*Memtable
	idGenerator                    *MemtableIdGenerator
	l0SSTableIds                   []uint64
	ssTables                       map[uint64]table.SSTable
	closeChannel                   chan struct{}
	flushMemtableCompletionChannel chan struct{}
	options                        StorageOptions
}

func NewStorageState() *StorageState {
	return NewStorageStateWithOptions(StorageOptions{
		MemTableSizeInBytes:   1 << 20,
		Path:                  ".",
		MaximumMemtables:      5,
		FlushMemtableDuration: 50 * time.Millisecond,
	})
}

func NewStorageStateWithOptions(options StorageOptions) *StorageState {
	if _, err := os.Stat(options.Path); os.IsNotExist(err) {
		_ = os.MkdirAll(options.Path, 0700)
	}
	idGenerator := NewMemtableIdGenerator()
	storageState := &StorageState{
		currentMemtable:                NewMemtable(idGenerator.NextId()),
		idGenerator:                    idGenerator,
		ssTables:                       make(map[uint64]table.SSTable),
		closeChannel:                   make(chan struct{}),
		flushMemtableCompletionChannel: make(chan struct{}),
		options:                        options,
	}
	storageState.spawnMemtableFlush()
	return storageState
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
	memtableIterators := func() []iterator.Iterator {
		iterators := make([]iterator.Iterator, len(storageState.immutableMemtables)+1)

		index := 0
		iterators[index] = storageState.currentMemtable.Scan(inclusiveRange)

		index += 1
		for immutableMemtableIndex := len(storageState.immutableMemtables) - 1; immutableMemtableIndex >= 0; immutableMemtableIndex-- {
			iterators[index] = storageState.immutableMemtables[immutableMemtableIndex].Scan(inclusiveRange)
			index += 1
		}
		return iterators
	}
	l0SSTableIterators := func() []iterator.Iterator {
		iterators := make([]iterator.Iterator, len(storageState.l0SSTableIds))
		index := 0

		for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
			ssTable := storageState.ssTables[storageState.l0SSTableIds[l0SSTableIndex]]
			ssTableIterator, err := ssTable.SeekToKey(inclusiveRange.Start())
			if err != nil {
				return nil
			}
			iterators[index] = ssTableIterator
			index += 1
		}
		return iterators
	}

	allIterators := append(memtableIterators(), l0SSTableIterators()...)
	return iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(allIterators), inclusiveRange.End())
}

func (storageState *StorageState) ForceFlushNextImmutableMemtable() error {
	var memtableToFlush *Memtable
	if len(storageState.immutableMemtables) > 0 {
		memtableToFlush = storageState.immutableMemtables[0]
	} else {
		panic("no immutable memtables available to flush")
	}

	buildSSTable := func() (table.SSTable, error) {
		ssTableBuilder := table.NewSSTableBuilderWithDefaultBlockSize()
		memtableToFlush.AllEntries(func(key txn.Key, value txn.Value) {
			ssTableBuilder.Add(key, value)
		})
		ssTable, err := ssTableBuilder.Build(
			memtableToFlush.id,
			filepath.Join(storageState.options.Path, fmt.Sprintf("%v.sst", memtableToFlush.id)),
		)
		if err != nil {
			return table.SSTable{}, err
		}
		return ssTable, nil
	}

	ssTable, err := buildSSTable()
	if err != nil {
		return err
	}
	storageState.immutableMemtables = storageState.immutableMemtables[1:]
	storageState.l0SSTableIds = append(storageState.l0SSTableIds, memtableToFlush.id) //TODO: Either use l0SSTables or levels
	storageState.ssTables[memtableToFlush.id] = ssTable
	//TODO: WAl remove, manifest, concurrency support
	return nil
}

// Close TODO: Complete the implementation
func (storageState *StorageState) Close() {
	close(storageState.closeChannel)
	//Wait for flush immutable tables goroutine to return
	<-storageState.flushMemtableCompletionChannel
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

// TODO: Manifest
// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable() {
	if storageState.currentMemtable.Size() >= storageState.options.MemTableSizeInBytes {
		storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
		storageState.currentMemtable = NewMemtable(storageState.idGenerator.NextId())
	}
}

// forceFreezeCurrentMemtable only for testing.
func (storageState *StorageState) forceFreezeCurrentMemtable() {
	storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
	storageState.currentMemtable = NewMemtable(storageState.idGenerator.NextId())
}

func (storageState *StorageState) spawnMemtableFlush() {
	timer := time.NewTimer(storageState.options.FlushMemtableDuration)
	go func() {
		for {
			select {
			case <-timer.C:
				if uint(len(storageState.immutableMemtables)) >= storageState.options.MaximumMemtables {
					if err := storageState.ForceFlushNextImmutableMemtable(); err != nil {
						panic(fmt.Errorf("could not flush memtable %v", err))
					}
				}
				timer.Reset(storageState.options.FlushMemtableDuration)
			case <-storageState.closeChannel:
				close(storageState.flushMemtableCompletionChannel)
				return
			}
		}
	}()
}
