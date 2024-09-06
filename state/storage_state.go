package state

import (
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/log"
	"go-lsm/manifest"
	"go-lsm/memory"
	"go-lsm/table"
	"log/slog"
	"os"
	"slices"
	"sort"
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
	compactionOptions     SimpleLeveledCompactionOptions
}

// StorageState TODO: Support concurrency and Close method
type StorageState struct {
	currentMemtable                *memory.Memtable
	immutableMemtables             []*memory.Memtable
	idGenerator                    *SSTableIdGenerator
	manifest                       *manifest.Manifest
	l0SSTableIds                   []uint64
	levels                         []*Level
	ssTables                       map[uint64]table.SSTable
	closeChannel                   chan struct{}
	flushMemtableCompletionChannel chan struct{}
	options                        StorageOptions
	walPath                        log.WALPath
	lastCommitTimestamp            uint64
}

// NewStorageStateWithOptions TODO: Recover from WAL
func NewStorageStateWithOptions(options StorageOptions) (*StorageState, error) {
	if _, err := os.Stat(options.Path); os.IsNotExist(err) {
		_ = os.MkdirAll(options.Path, os.ModePerm)
	}
	levels := make([]*Level, options.compactionOptions.maxLevels)
	for level := 1; level <= int(options.compactionOptions.maxLevels); level++ {
		levels[level-1] = &Level{levelNumber: level}
	}
	manifestRecorder, events, err := manifest.CreateNewOrRecoverFrom(options.Path)
	if err != nil {
		return nil, err
	}

	storageState := &StorageState{
		idGenerator:                    NewSSTableIdGenerator(),
		manifest:                       manifestRecorder,
		ssTables:                       make(map[uint64]table.SSTable),
		levels:                         levels,
		closeChannel:                   make(chan struct{}),
		flushMemtableCompletionChannel: make(chan struct{}),
		options:                        options,
		walPath:                        log.NewWALPath(options.Path),
		lastCommitTimestamp:            0,
	}
	if err := storageState.mayBeLoadExisting(events); err != nil {
		return nil, err
	}
	storageState.spawnMemtableFlush()
	return storageState, nil
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
func (storageState *StorageState) Set(timestampedBatch kv.TimestampedBatch) error {
	if err := storageState.mayBeFreezeCurrentMemtable(int64(timestampedBatch.SizeInBytes())); err != nil {
		return err
	}
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
	return nil
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
			storageState.options.Path,
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
	if err := storageState.manifest.Add(manifest.NewSSTableFlushed(ssTable.Id())); err != nil {
		return err
	}
	memtableToFlush.DeleteWAL()

	//TODO: concurrency support
	return nil
}

// Close TODO: Complete the implementation
func (storageState *StorageState) Close() {
	close(storageState.closeChannel)
	//Wait for flush immutable tables goroutine to return
	<-storageState.flushMemtableCompletionChannel
}

func (storageState *StorageState) WALDirectoryPath() string {
	return storageState.walPath.DirectoryPath
}

func (storageState *StorageState) LastCommitTimestamp() uint64 {
	return storageState.lastCommitTimestamp
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

func (storageState *StorageState) sortedMemtableIds() []uint64 {
	ids := make([]uint64, 0, 1+len(storageState.immutableMemtables))
	ids = append(ids, storageState.currentMemtable.Id())
	for _, immutableMemtable := range storageState.immutableMemtables {
		ids = append(ids, immutableMemtable.Id())
	}
	slices.Sort(ids)
	return ids
}

// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable(requiredSizeInBytes int64) error {
	if !storageState.currentMemtable.CanFit(requiredSizeInBytes) {
		storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
		storageState.currentMemtable = memory.NewMemtable(
			storageState.idGenerator.NextId(),
			storageState.options.MemTableSizeInBytes,
			storageState.walPath,
		)
		return storageState.manifest.Add(manifest.NewMemtableCreated(storageState.currentMemtable.Id()))
	}
	return nil
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
						slog.Error("could not flush memtable, error: %v", err)
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

func (storageState *StorageState) mayBeLoadExisting(events []manifest.Event) error {
	if len(events) > 0 {
		memtableIds := make(map[uint64]struct{})
		for _, event := range events {
			switch event.EventType() {
			case manifest.MemtableCreatedEventType:
				memtableCreated := event.(*manifest.MemtableCreated)
				memtableIds[memtableCreated.MemtableId] = struct{}{}
				storageState.idGenerator.setIdIfGreaterThanExisting(memtableCreated.MemtableId)
			default:
				panic("unhandled default case")
			}
		}
		if err := storageState.recoverMemtables(memtableIds); err != nil {
			return err
		}
	}
	storageState.currentMemtable = memory.NewMemtable(
		storageState.idGenerator.NextId(),
		storageState.options.MemTableSizeInBytes,
		storageState.walPath,
	)
	if err := storageState.manifest.Add(manifest.NewMemtableCreated(storageState.currentMemtable.Id())); err != nil {
		return err
	}
	return nil
}

func (storageState *StorageState) recoverMemtables(memtableIds map[uint64]struct{}) error {
	var immutableMemtables []*memory.Memtable
	var maxTimestamp uint64

	for memtableId := range memtableIds {
		memtable, timestamp, err := memory.RecoverFromWAL(
			memtableId,
			storageState.options.MemTableSizeInBytes,
			storageState.WALDirectoryPath(),
		)
		if err != nil {
			return err
		}
		immutableMemtables = append(immutableMemtables, memtable)
		maxTimestamp = max(maxTimestamp, timestamp)
	}
	sort.Slice(immutableMemtables, func(i, j int) bool {
		return immutableMemtables[i].Id() < immutableMemtables[j].Id()
	})
	storageState.lastCommitTimestamp = maxTimestamp
	storageState.immutableMemtables = immutableMemtables
	return nil
}
