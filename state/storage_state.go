package state

import (
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/log"
	"go-lsm/manifest"
	"go-lsm/memory"
	"go-lsm/table"
	"go-lsm/table/block"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

type SimpleLeveledCompactionOptions struct {
	NumberOfSSTablesRatioPercentage uint
	MaxLevels                       uint
	Level0FilesCompactionTrigger    uint
}

type StorageOptions struct {
	MemTableSizeInBytes   int64
	SSTableSizeInBytes    int64
	Path                  string
	MaximumMemtables      uint
	FlushMemtableDuration time.Duration
	CompactionOptions     SimpleLeveledCompactionOptions
}

// StorageState TODO: Support concurrency and Close method, populate levels fields also (refer simple_leveled in compact)
type StorageState struct {
	currentMemtable                *memory.Memtable
	immutableMemtables             []*memory.Memtable
	idGenerator                    *SSTableIdGenerator
	manifest                       *manifest.Manifest
	ssTableCleaner                 *table.SSTableCleaner
	l0SSTableIds                   []uint64
	levels                         []*Level
	ssTables                       map[uint64]*table.SSTable
	closeChannel                   chan struct{}
	flushMemtableCompletionChannel chan struct{}
	options                        StorageOptions
	walPath                        log.WALPath
	lastCommitTimestamp            uint64
	stateLock                      sync.RWMutex
}

// NewStorageStateWithOptions TODO: Recover from WAL
func NewStorageStateWithOptions(options StorageOptions) (*StorageState, error) {
	if _, err := os.Stat(options.Path); os.IsNotExist(err) {
		_ = os.MkdirAll(options.Path, os.ModePerm)
	}
	levels := make([]*Level, options.CompactionOptions.MaxLevels)
	for level := 1; level <= int(options.CompactionOptions.MaxLevels); level++ {
		levels[level-1] = &Level{LevelNumber: level}
	}
	manifestRecorder, events, err := manifest.CreateNewOrRecoverFrom(options.Path)
	if err != nil {
		return nil, err
	}

	storageState := &StorageState{
		idGenerator:                    NewSSTableIdGenerator(),
		manifest:                       manifestRecorder,
		ssTableCleaner:                 table.NewSSTableCleaner(5 * time.Millisecond),
		ssTables:                       make(map[uint64]*table.SSTable),
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
	storageState.ssTableCleaner.Start()
	return storageState, nil
}

func (storageState *StorageState) Get(key kv.Key) (kv.Value, bool) {
	storageState.stateLock.RLock()
	defer storageState.stateLock.RUnlock()

	enquireMemtables := func() (kv.Value, bool) {
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
		return kv.EmptyValue, false
	}
	enquireL0SSTables := func() (kv.Value, bool) {
		l0SSTableIterators, onCloseCallback := storageState.l0SSTableIterators(key, func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(kv.NewInclusiveKeyRange(key, key)) && ssTable.MayContain(key)
		})
		boundedIterator := iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(l0SSTableIterators, onCloseCallback), key)
		defer boundedIterator.Close()

		if boundedIterator.IsValid() && boundedIterator.Key().IsRawKeyEqualTo(key) {
			return boundedIterator.Value(), true
		}
		return kv.EmptyValue, false
	}
	enquireOtherLevelSSTables := func() (kv.Value, bool) {
		l0SSTableIterators, onCloseCallback := storageState.otherLevelSSTableIterators(key, func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(kv.NewInclusiveKeyRange(key, key)) && ssTable.MayContain(key)
		})
		boundedIterator := iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(l0SSTableIterators, onCloseCallback), key)
		defer boundedIterator.Close()

		if boundedIterator.IsValid() && boundedIterator.Key().IsRawKeyEqualTo(key) {
			return boundedIterator.Value(), true
		}
		return kv.EmptyValue, false
	}

	if value, ok := enquireMemtables(); ok {
		return value, true
	}
	if value, ok := enquireL0SSTables(); ok {
		return value, true
	}
	if value, ok := enquireOtherLevelSSTables(); ok {
		return value, true
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
	storageState.stateLock.RLock()
	defer storageState.stateLock.RUnlock()

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

	l0SSTableIterators, onCloseCallback := storageState.l0SSTableIterators(inclusiveRange.Start(), func(ssTable *table.SSTable) bool {
		return ssTable.ContainsInclusive(inclusiveRange)
	})
	allIterators := append(memtableIterators(), l0SSTableIterators...)
	return iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(allIterators, onCloseCallback), inclusiveRange.End())
}

func (storageState *StorageState) Apply(event StorageStateChangeEvent, recovery bool) error {
	ssTablesToRemove := storageState.apply(event)
	if !recovery {
		if err := storageState.manifest.Add(manifest.NewCompactionDone(event.NewSSTableIds, event.CompactionDescription())); err != nil {
			return err
		}
	}
	storageState.ssTableCleaner.Submit(ssTablesToRemove)
	return nil
}

func (storageState *StorageState) SSTableIdGenerator() *SSTableIdGenerator {
	return storageState.idGenerator
}

func (storageState *StorageState) Options() StorageOptions {
	return storageState.options
}

func (storageState *StorageState) WALDirectoryPath() string {
	return storageState.walPath.DirectoryPath
}

func (storageState *StorageState) LastCommitTimestamp() uint64 {
	return storageState.lastCommitTimestamp
}

func (storageState *StorageState) Snapshot() StorageStateSnapshot {
	storageState.stateLock.RLock()
	defer storageState.stateLock.RUnlock()

	return StorageStateSnapshot{
		L0SSTableIds: storageState.l0SSTableIds, //TODO: order it?
		Levels:       storageState.levels,
		SSTables:     storageState.ssTables,
	}
}

// Close TODO: Complete the implementation
func (storageState *StorageState) Close() {
	close(storageState.closeChannel)
	//Wait for flush immutable tables goroutine to return
	<-storageState.flushMemtableCompletionChannel
	//Wait for ssTableCleaner to return
	<-storageState.ssTableCleaner.Stop()
}

func (storageState *StorageState) ForceFlushNextImmutableMemtable() error {
	flushEligibleMemtable := func() *memory.Memtable {
		storageState.stateLock.Lock()
		defer storageState.stateLock.Unlock()

		var memtable *memory.Memtable
		if len(storageState.immutableMemtables) > 0 {
			memtable = storageState.immutableMemtables[0]
		} else {
			panic("no immutable memtables available to flush")
		}
		return memtable
	}
	buildSSTable := func(memtableToFlush *memory.Memtable) (*table.SSTable, error) {
		ssTableBuilder := table.NewSSTableBuilderWithDefaultBlockSize()
		memtableToFlush.AllEntries(func(key kv.Key, value kv.Value) {
			ssTableBuilder.Add(key, value)
		})
		ssTable, err := ssTableBuilder.Build(
			memtableToFlush.Id(),
			storageState.options.Path,
		)
		if err != nil {
			return nil, err
		}
		return ssTable, nil
	}

	memtableToFlush := flushEligibleMemtable()
	ssTable, err := buildSSTable(memtableToFlush)
	if err != nil {
		return err
	}

	storageState.stateLock.Lock()
	storageState.immutableMemtables = storageState.immutableMemtables[1:]
	storageState.l0SSTableIds = append(storageState.l0SSTableIds, memtableToFlush.Id()) //TODO: Either use l0SSTables or Levels
	storageState.ssTables[memtableToFlush.Id()] = ssTable
	storageState.stateLock.Unlock()

	if err := storageState.manifest.Add(manifest.NewSSTableFlushed(ssTable.Id())); err != nil {
		return err
	}
	memtableToFlush.DeleteWAL()

	return nil
}

// TODO: Sync WAL of the old memtable (If Memtable gets a WAL)
// TODO: When concurrency comes in, ensure mayBeFreezeCurrentMemtable is called by one goroutine only
func (storageState *StorageState) mayBeFreezeCurrentMemtable(requiredSizeInBytes int64) error {
	if !storageState.currentMemtable.CanFit(requiredSizeInBytes) {
		storageState.stateLock.Lock()
		storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
		storageState.currentMemtable = memory.NewMemtable(
			storageState.idGenerator.NextId(),
			storageState.options.MemTableSizeInBytes,
			storageState.walPath,
		)
		storageState.stateLock.Unlock()
		return storageState.manifest.Add(manifest.NewMemtableCreated(storageState.currentMemtable.Id()))
	}
	return nil
}

func (storageState *StorageState) l0SSTableIterators(seekTo kv.Key, ssTableSelector func(ssTable *table.SSTable) bool) ([]iterator.Iterator, iterator.OnCloseCallback) {
	iterators := make([]iterator.Iterator, len(storageState.l0SSTableIds))
	index := 0

	var ssTablesInUse []*table.SSTable
	for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
		ssTable := storageState.ssTables[storageState.l0SSTableIds[l0SSTableIndex]]
		if ssTableSelector(ssTable) {
			ssTableIterator, err := ssTable.SeekToKey(seekTo)
			if err != nil {
				return nil, iterator.NoOperationOnCloseCallback
			}
			ssTablesInUse = append(ssTablesInUse, ssTable)
			iterators[index] = ssTableIterator
			index += 1
		}
	}
	return iterators, func() {
		table.DecrementReferenceFor(ssTablesInUse)
	}
}

func (storageState *StorageState) otherLevelSSTableIterators(seekTo kv.Key, ssTableSelector func(ssTable *table.SSTable) bool) ([]iterator.Iterator, iterator.OnCloseCallback) {
	var ssTablesInUse []*table.SSTable
	var iterators []iterator.Iterator

	for _, level := range storageState.levels {
		for _, ssTableId := range level.SSTableIds {
			ssTable := storageState.ssTables[ssTableId]
			if ssTableSelector(ssTable) {
				ssTableIterator, err := ssTable.SeekToKey(seekTo)
				if err != nil {
					return nil, iterator.NoOperationOnCloseCallback
				}
				ssTablesInUse = append(ssTablesInUse, ssTable)
				iterators = append(iterators, ssTableIterator)
			}
		}
	}
	return iterators, func() {
		table.DecrementReferenceFor(ssTablesInUse)
	}
}

func (storageState *StorageState) spawnMemtableFlush() {
	hasImmutableMemtablesGoneBeyondMaximumAllowed := func() bool {
		storageState.stateLock.RLock()
		defer storageState.stateLock.RUnlock()

		return uint(len(storageState.immutableMemtables)) >= storageState.options.MaximumMemtables
	}

	timer := time.NewTimer(storageState.options.FlushMemtableDuration)
	go func() {
		for {
			select {
			case <-timer.C:
				if hasImmutableMemtablesGoneBeyondMaximumAllowed() {
					if err := storageState.ForceFlushNextImmutableMemtable(); err != nil {
						slog.Error("could not flush memtable, error: %v", err)
					}
				}
				timer.Reset(storageState.options.FlushMemtableDuration)
			case <-storageState.closeChannel:
				close(storageState.flushMemtableCompletionChannel)
				timer.Stop()
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
			case manifest.SSTableFlushedEventType:
				ssTableFlushed := event.(*manifest.SSTableFlushed)
				delete(memtableIds, ssTableFlushed.SsTableId)
				storageState.l0SSTableIds = append(storageState.l0SSTableIds, ssTableFlushed.SsTableId)
				storageState.idGenerator.setIdIfGreaterThanExisting(ssTableFlushed.SsTableId)
			case manifest.CompactionDoneEventType:
				compactionDone := event.(*manifest.CompactionDone)
				storageChangeEvent, err := NewStorageStateChangeEventByOpeningSSTables(
					compactionDone.NewSSTableIds,
					compactionDone.Description,
					storageState.options.Path,
				)
				oldSSTableIds := compactionDone.Description.UpperLevelSSTableIds
				oldSSTableIds = append(oldSSTableIds, compactionDone.Description.LowerLevelSSTableIds...)

				for _, ssTableId := range oldSSTableIds {
					ssTable, err := table.Load(ssTableId, storageState.options.Path, block.DefaultBlockSize)
					if err == nil {
						storageState.ssTables[ssTable.Id()] = ssTable
					}
				}
				if err != nil {
					return err
				}
				if err := storageState.Apply(storageChangeEvent, true); err != nil {
					return err
				}
				storageState.idGenerator.setIdIfGreaterThanExisting(storageChangeEvent.MaxSSTableId())
			}
		}
		if err := storageState.recoverL0SSTables(); err != nil {
			return err
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
		if !memtable.IsEmpty() {
			immutableMemtables = append(immutableMemtables, memtable)
		}
		maxTimestamp = max(maxTimestamp, timestamp)
	}
	sort.Slice(immutableMemtables, func(i, j int) bool {
		return immutableMemtables[i].Id() < immutableMemtables[j].Id()
	})
	storageState.lastCommitTimestamp = maxTimestamp
	storageState.immutableMemtables = immutableMemtables
	return nil
}

func (storageState *StorageState) recoverL0SSTables() error {
	for _, ssTableId := range storageState.l0SSTableIds {
		ssTable, err := table.Load(ssTableId, storageState.options.Path, block.DefaultBlockSize)
		if err != nil {
			return err
		}
		storageState.ssTables[ssTable.Id()] = ssTable
	}
	return nil
}

func (storageState *StorageState) apply(event StorageStateChangeEvent) []*table.SSTable {
	storageState.stateLock.Lock()
	defer storageState.stateLock.Unlock()

	type SSTablesToRemove = []*table.SSTable
	setSSTableMapping := func() {
		for _, ssTable := range event.NewSSTables {
			storageState.ssTables[ssTable.Id()] = ssTable
		}
	}
	updateLevels := func() []uint64 {
		var ssTableIdsToRemove []uint64
		if event.CompactionUpperLevel() == -1 {
			ssTableIdsToRemove = append(ssTableIdsToRemove, event.CompactionUpperLevelSSTableIds()...)
			storageState.l0SSTableIds = event.allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds(storageState.l0SSTableIds)
		} else {
			ssTableIdsToRemove = append(ssTableIdsToRemove, storageState.levels[event.CompactionUpperLevel()-1].SSTableIds...)
			storageState.levels[event.CompactionUpperLevel()-1].clearSSTableIds()
		}
		ssTableIdsToRemove = append(ssTableIdsToRemove, event.CompactionLowerLevelSSTableIds()...)
		storageState.levels[event.CompactionLowerLevel()-1].appendSSTableIds(event.NewSSTableIds)

		return ssTableIdsToRemove
	}
	unsetSSTableMapping := func(ssTableIdsToRemove []uint64) SSTablesToRemove {
		var ssTables = make(SSTablesToRemove, 0, len(ssTableIdsToRemove))
		for _, ssTableId := range ssTableIdsToRemove {
			ssTable, ok := storageState.ssTables[ssTableId]
			if ok {
				ssTables = append(ssTables, ssTable)
			}
			delete(storageState.ssTables, ssTableId)
		}
		return ssTables
	}
	setSSTableMapping()
	return unsetSSTableMapping(updateLevels())
}
