package state

import (
	"fmt"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/memory"
	"go-lsm/table"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	level0 = iota
	level1 = 1
)
const totalLevels = 6

type Level struct {
	levelNumber int
	ssTableIds  []uint64
}

type StorageOptions struct {
	MemTableSizeInBytes   int64
	SSTableSizeInBytes    int64 //TODO: Do we need it?
	Path                  string
	MaximumMemtables      uint
	FlushMemtableDuration time.Duration
	EnableWAL             bool
	compactionOptions     SimpleLeveledCompactionOptions
}

// StorageState TODO: Support concurrency and Close method
type StorageState struct {
	currentMemtable                *memory.Memtable
	immutableMemtables             []*memory.Memtable
	idGenerator                    *SSTableIdGenerator
	l0SSTableIds                   []uint64
	levels                         []*Level
	ssTables                       map[uint64]table.SSTable
	closeChannel                   chan struct{}
	flushMemtableCompletionChannel chan struct{}
	options                        StorageOptions
	walDirectoryPath               string
}

func NewStorageState() *StorageState {
	return NewStorageStateWithOptions(StorageOptions{
		MemTableSizeInBytes:   1 << 20,
		Path:                  ".",
		MaximumMemtables:      5,
		FlushMemtableDuration: 50 * time.Millisecond,
		EnableWAL:             false,
		compactionOptions: SimpleLeveledCompactionOptions{
			level0FilesCompactionTrigger: 6,
			maxLevels:                    totalLevels,
			sizeRatioPercentage:          200,
		},
	})
}

// NewStorageStateWithOptions TODO: Recover from WAL
func NewStorageStateWithOptions(options StorageOptions) *StorageState {
	if _, err := os.Stat(options.Path); os.IsNotExist(err) {
		_ = os.MkdirAll(options.Path, os.ModePerm)
	}
	walDirectoryPath := filepath.Join(options.Path, "wal")
	if _, err := os.Stat(walDirectoryPath); os.IsNotExist(err) {
		_ = os.MkdirAll(walDirectoryPath, os.ModePerm)
	}
	levels := make([]*Level, options.compactionOptions.maxLevels)
	for level := 1; level <= int(options.compactionOptions.maxLevels); level++ {
		levels[level-1] = &Level{levelNumber: level}
	}
	idGenerator := NewSSTableIdGenerator()
	storageState := &StorageState{
		currentMemtable:                memory.NewMemtable(idGenerator.NextId(), options.MemTableSizeInBytes, memory.NewWALPresence(options.EnableWAL, walDirectoryPath)),
		idGenerator:                    idGenerator,
		ssTables:                       make(map[uint64]table.SSTable),
		levels:                         levels,
		closeChannel:                   make(chan struct{}),
		flushMemtableCompletionChannel: make(chan struct{}),
		options:                        options,
		walDirectoryPath:               walDirectoryPath,
	}
	storageState.spawnMemtableFlush()
	return storageState
}

func (storageState *StorageState) Get(key kv.Key) (kv.Value, bool) {
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

	mergeIterator := iterator.NewInclusiveBoundedIterator(
		iterator.NewMergeIterator(storageState.l0SSTableIterators(
			key,
			func(ssTable table.SSTable) bool {
				return ssTable.ContainsInclusive(kv.NewInclusiveKeyRange(key, key)) && ssTable.MayContain(key)
			},
		)),
		key,
	)
	if mergeIterator.IsValid() && mergeIterator.Key().IsRawKeyEqualTo(key) {
		return mergeIterator.Value(), true
	}
	return kv.EmptyValue, false
}

// Set
// TODO: Handle error in Set and Delete
func (storageState *StorageState) Set(timestampedBatch kv.TimestampedBatch) {
	storageState.mayBeFreezeCurrentMemtable(int64(timestampedBatch.SizeInBytes()))
	for _, entry := range timestampedBatch.AllEntries() {
		if entry.IsKindPut() {
			_ = storageState.currentMemtable.Set(entry.Key, entry.Value)
		} else if entry.IsKindDelete() {
			_ = storageState.currentMemtable.Delete(entry.Key)
		} else {
			panic("Unsupported entry type")
		}
	}
	storageState.currentMemtable.Sync()
}

func (storageState *StorageState) Scan(inclusiveRange kv.InclusiveKeyRange[kv.Key]) iterator.Iterator {
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

	allIterators := append(
		memtableIterators(),
		storageState.l0SSTableIterators(inclusiveRange.Start(), func(ssTable table.SSTable) bool {
			return ssTable.ContainsInclusive(inclusiveRange)
		})...,
	)
	return iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(allIterators), inclusiveRange.End())
}

func (storageState *StorageState) ForceFlushNextImmutableMemtable() error {
	var memtableToFlush *memory.Memtable
	if len(storageState.immutableMemtables) > 0 {
		memtableToFlush = storageState.immutableMemtables[0]
	} else {
		panic("no immutable memtables available to flush")
	}

	buildSSTable := func() (table.SSTable, error) {
		ssTableBuilder := table.NewSSTableBuilderWithDefaultBlockSize()
		memtableToFlush.AllEntries(func(key kv.Key, value kv.Value) {
			ssTableBuilder.Add(key, value)
		})
		ssTable, err := ssTableBuilder.Build(
			memtableToFlush.Id(),
			storageState.ssTableFilePath(memtableToFlush.Id()),
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
	storageState.l0SSTableIds = append(storageState.l0SSTableIds, memtableToFlush.Id()) //TODO: Either use l0SSTables or levels
	storageState.ssTables[memtableToFlush.Id()] = ssTable
	//TODO: WAl remove, manifest, concurrency support
	return nil
}

// Close TODO: Complete the implementation
func (storageState *StorageState) Close() {
	close(storageState.closeChannel)
	//Wait for flush immutable tables goroutine to return
	<-storageState.flushMemtableCompletionChannel
}

func (storageState *StorageState) orderedSSTableIds(level int) []uint64 {
	if level == 0 {
		ids := make([]uint64, 0, len(storageState.l0SSTableIds))
		for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
			ids = append(ids, storageState.l0SSTableIds[l0SSTableIndex])
		}
		return ids
	}
	ssTableIds := storageState.levels[level-1].ssTableIds
	ids := make([]uint64, 0, len(ssTableIds))
	for ssTableIndex := len(ssTableIds) - 1; ssTableIndex >= 0; ssTableIndex-- {
		ids = append(ids, ssTableIds[ssTableIndex])
	}
	return ids
}

func (storageState *StorageState) ssTableFilePath(id uint64) string {
	return filepath.Join(storageState.options.Path, fmt.Sprintf("%v.sst", id))
}

func (storageState *StorageState) hasImmutableMemtables() bool {
	return len(storageState.immutableMemtables) > 0
}

func (storageState *StorageState) sortedMemtableIds() []uint64 {
	ids := make([]uint64, 0, 1+len(storageState.immutableMemtables))
	ids = append(ids, storageState.currentMemtable.Id())
	for _, immutableMemtable := range storageState.immutableMemtables {
		ids = append(ids, immutableMemtable.Id())
	}
	slices.Sort(ids)
	return ids
}

// TODO: Manifest
// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable(requiredSizeInBytes int64) {
	if !storageState.currentMemtable.CanFit(requiredSizeInBytes) {
		storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
		storageState.currentMemtable = memory.NewMemtable(
			storageState.idGenerator.NextId(),
			storageState.options.MemTableSizeInBytes,
			memory.NewWALPresence(storageState.options.EnableWAL, storageState.walDirectoryPath),
		)
	}
}

// forceFreezeCurrentMemtable only for testing.
func (storageState *StorageState) forceFreezeCurrentMemtable() {
	storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
	storageState.currentMemtable = memory.NewMemtable(
		storageState.idGenerator.NextId(),
		storageState.options.MemTableSizeInBytes,
		memory.NewWALPresence(storageState.options.EnableWAL, storageState.walDirectoryPath),
	)
}

func (storageState *StorageState) l0SSTableIterators(seekTo kv.Key, ssTableSelector func(ssTable table.SSTable) bool) []iterator.Iterator {
	iterators := make([]iterator.Iterator, len(storageState.l0SSTableIds))
	index := 0

	for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
		ssTable := storageState.ssTables[storageState.l0SSTableIds[l0SSTableIndex]]
		if ssTableSelector(ssTable) {
			ssTableIterator, err := ssTable.SeekToKey(seekTo)
			if err != nil {
				return nil
			}
			iterators[index] = ssTableIterator
			index += 1
		}
	}
	return iterators
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
