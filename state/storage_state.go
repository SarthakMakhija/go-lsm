package state

import (
	"fmt"
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

// CompactionOptions represents a combination of SimpleLeveledCompactionOptions and
// the duration at which compaction goroutine should run.
type CompactionOptions struct {
	StrategyOptions SimpleLeveledCompactionOptions
	Duration        time.Duration
}

// SimpleLeveledCompactionOptions represents the configurable options for simple-leveled compaction.
// Read more about the logic behind simple-leveled compaction in compact.SimpleLeveledCompaction.
type SimpleLeveledCompactionOptions struct {
	NumberOfSSTablesRatioPercentage uint
	MaxLevels                       uint
	Level0FilesCompactionTrigger    uint
}

// StorageOptions represents the configuration options for StorageState.
type StorageOptions struct {
	MemTableSizeInBytes   int64
	SSTableSizeInBytes    int64
	Path                  string
	MaximumMemtables      uint
	FlushMemtableDuration time.Duration
	CompactionOptions     CompactionOptions
}

// StorageState represents the core abstraction to manage the in-memory state of the key/value storage engine.
type StorageState struct {
	currentMemtable *memory.Memtable
	//oldest to latest immutable memtable.
	immutableMemtables []*memory.Memtable
	idGenerator        *SSTableIdGenerator
	manifest           *manifest.Manifest
	ssTableCleaner     *table.SSTableCleaner
	//oldest to latest level0 SStable ids.
	l0SSTableIds                   []uint64
	levels                         []*Level
	ssTables                       map[uint64]*table.SSTable
	closeChannel                   chan struct{}
	flushMemtableCompletionChannel chan struct{}
	options                        StorageOptions
	walPath                        log.WALPath
	lastCommitTimestamp            uint64
	//stateLock is needed because compaction might cause a change in the StorageState (Refer to the Apply() method).
	//Had compaction not been there, stateLock was not needed because the transaction isolation is serialized-snapshot, which means
	//all the writes are written serially, and reads are based on read-timestamp, which means both these operations can run
	//concurrently.
	stateLock sync.RWMutex
}

// NewStorageStateWithOptions creates new instance of StorageState, or loads the existing state from manifest.Manifest.
func NewStorageStateWithOptions(options StorageOptions) (*StorageState, error) {
	if _, err := os.Stat(options.Path); os.IsNotExist(err) {
		_ = os.MkdirAll(options.Path, os.ModePerm)
	}
	levels := make([]*Level, options.CompactionOptions.StrategyOptions.MaxLevels)
	for level := 1; level <= int(options.CompactionOptions.StrategyOptions.MaxLevels); level++ {
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

// Get gets the value of the given key from the current memtable, followed by immutable memtables,
// level0 SSTables and then finally SSTables from different levels.
// An important point in Get and Scan is decrementing the references for the SSTables in use.
// It is quite possible that at time T1 SSTables A and B are used for performing a Scan operation.
// At time T2 (T2 > T1), compaction runs and the outcome of compaction is to clean SSTable A and B.
// However, SSTables A and B are still being referred by some transaction which involves Scan operation.
// Unless the reference count of SSTables A and B drops to zero, these tables can not be cleaned.
// Refer to: table.SSTable, table.SSTableCleaner.
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
		l0SSTableIterators, ssTablesInUse := storageState.l0SSTableIterators(key, func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(kv.NewInclusiveKeyRange(key, key)) && ssTable.MayContain(key)
		})
		boundedIterator := iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(l0SSTableIterators, func() {
			table.DecrementReferenceFor(ssTablesInUse)
		}), key)
		defer boundedIterator.Close()

		if boundedIterator.IsValid() && boundedIterator.Key().IsRawKeyEqualTo(key) {
			return boundedIterator.Value(), true
		}
		return kv.EmptyValue, false
	}
	enquireOtherLevelSSTables := func() (kv.Value, bool) {
		otherSSTableIterators, ssTablesInUse := storageState.otherLevelSSTableIterators(key, func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(kv.NewInclusiveKeyRange(key, key)) && ssTable.MayContain(key)
		})
		boundedIterator := iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(otherSSTableIterators, func() {
			table.DecrementReferenceFor(ssTablesInUse)
		}), key)
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

// Set sets the kv.TimestampedBatch in the memtable.
// If the current memtable can not accommodate the incoming batch, it is frozen and a new memtable is created.
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

// Scan performs a forward scan for the kv.InclusiveKeyRange.
// It involves creating iterators from the current memtable, followed by immutable memtables,
// level0 SSTables and then finally SSTables from different levels.
// It finally returns an instance of iterator.NewInclusiveBoundedIterator which returns the latest version (/timestamp) of any key.
// An important point in Get and Scan is decrementing the references for the SSTables in use.
// It is quite possible that at time T1 SSTables A and B are used for performing a Scan operation.
// At time T2 (T2 > T1), compaction runs and the outcome of compaction is to clean SSTable A and B.
// However, SSTables A and B are still being referred by some transaction which involves Scan operation.
// Unless the reference count of SSTables A and B drops to zero, these tables can not be cleaned.
// Refer to: table.SSTable, table.SSTableCleaner.
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
	ssTableIteratorsAtAllLevels := func() ([]iterator.Iterator, []*table.SSTable) {
		l0SSTableIterators, ssTablesFromLevel0InUse := storageState.l0SSTableIterators(inclusiveRange.Start(), func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(inclusiveRange)
		})
		otherSSTableIterators, ssTablesFromOtherLevelsInUse := storageState.otherLevelSSTableIterators(inclusiveRange.Start(), func(ssTable *table.SSTable) bool {
			return ssTable.ContainsInclusive(inclusiveRange)
		})
		return append(l0SSTableIterators, otherSSTableIterators...), append(ssTablesFromLevel0InUse, ssTablesFromOtherLevelsInUse...)
	}

	ssTableIterators, ssTablesInUse := ssTableIteratorsAtAllLevels()
	return iterator.NewInclusiveBoundedIterator(iterator.NewMergeIterator(append(memtableIterators(), ssTableIterators...), func() {
		table.DecrementReferenceFor(ssTablesInUse)
	}), inclusiveRange.End())
}

// Apply applies the StorageStateChangeEvent to the StorageState.
// It is called if compaction runs between two adjacent levels.
// Applying StorageStateChangeEvent is exclusive, as it requires a write-lock.
// As a part of applying the StorageStateChangeEvent, all the table.SSTable(s) which are to be removed are submitted to
// table.SSTableCleaner.
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

// SSTableIdGenerator returns the SSTableIdGenerator.
func (storageState *StorageState) SSTableIdGenerator() *SSTableIdGenerator {
	return storageState.idGenerator
}

// Options returns the StorageOptions.
func (storageState *StorageState) Options() StorageOptions {
	return storageState.options
}

// WALDirectoryPath returns the directory path of WAL.
func (storageState *StorageState) WALDirectoryPath() string {
	return storageState.walPath.DirectoryPath
}

// LastCommitTimestamp returns the last commit-timestamp which is recovered from WAL.
func (storageState *StorageState) LastCommitTimestamp() uint64 {
	return storageState.lastCommitTimestamp
}

// Snapshot returns the point-in-time state of StorageState.
func (storageState *StorageState) Snapshot() StorageStateSnapshot {
	storageState.stateLock.RLock()
	defer storageState.stateLock.RUnlock()

	return StorageStateSnapshot{
		L0SSTableIds: storageState.orderedLevel0SSTableIds(),
		Levels:       storageState.levels,
		SSTables:     storageState.ssTables,
	}
}

// Close closes the StorageState.
func (storageState *StorageState) Close() {
	close(storageState.closeChannel)
	//Wait for flush immutable tables goroutine to return
	<-storageState.flushMemtableCompletionChannel
	//Wait for ssTableCleaner to return
	<-storageState.ssTableCleaner.Stop()
}

// forceFlushNextImmutableMemtable flushes the next immutable memtable to level0 table.SSTable.
// It picks the oldest memtable from immutableMemtables fields to be flushed and records the manifest.SSTableFlushedEventType
// event in manifest.Manifest.
func (storageState *StorageState) forceFlushNextImmutableMemtable() error {
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
	storageState.l0SSTableIds = append(storageState.l0SSTableIds, memtableToFlush.Id())
	storageState.ssTables[memtableToFlush.Id()] = ssTable
	storageState.stateLock.Unlock()

	if err := storageState.manifest.Add(manifest.NewSSTableFlushed(ssTable.Id())); err != nil {
		return err
	}
	memtableToFlush.DeleteWAL()

	return nil
}

// mayBeFreezeCurrentMemtable may freeze the current memtable if the current memtable does not have required size.
// It may result in creation of a new memtable which is then recorded as manifest.MemtableCreatedEventType in manifest.Manifest.
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

// l0SSTableIterators returns all a slice of iterator.Iterator from level0 table.SSTable(s), along with a slice of
// all the table.SSTable(s) in use.
// Iterators are created from the latest memtable to the oldest (from index = len(storageState.l0SSTableIds) to index = 0).
func (storageState *StorageState) l0SSTableIterators(seekTo kv.Key, ssTableSelector func(ssTable *table.SSTable) bool) ([]iterator.Iterator, []*table.SSTable) {
	iterators := make([]iterator.Iterator, len(storageState.l0SSTableIds))
	index := 0

	var ssTablesInUse []*table.SSTable
	for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
		ssTable := storageState.ssTables[storageState.l0SSTableIds[l0SSTableIndex]]
		if ssTableSelector(ssTable) {
			ssTableIterator, err := ssTable.SeekToKey(seekTo)
			if err != nil {
				return nil, nil
			}
			ssTablesInUse = append(ssTablesInUse, ssTable)
			iterators[index] = ssTableIterator
			index += 1
		}
	}
	return iterators, ssTablesInUse
}

// otherLevelSSTableIterators returns all a slice of iterator.Iterator from table.SSTable(s) present in every level other than level0,
// along with a slice of all the table.SSTable(s) in use.
func (storageState *StorageState) otherLevelSSTableIterators(seekTo kv.Key, ssTableSelector func(ssTable *table.SSTable) bool) ([]iterator.Iterator, []*table.SSTable) {
	var ssTablesInUse []*table.SSTable
	var iterators []iterator.Iterator

	for _, level := range storageState.levels {
		for _, ssTableId := range level.SSTableIds {
			ssTable := storageState.ssTables[ssTableId]
			if ssTableSelector(ssTable) {
				ssTableIterator, err := ssTable.SeekToKey(seekTo)
				if err != nil {
					return nil, nil
				}
				ssTablesInUse = append(ssTablesInUse, ssTable)
				iterators = append(iterators, ssTableIterator)
			}
		}
	}
	return iterators, ssTablesInUse
}

// spawnMemtableFlush creates a goroutine which flushes the oldest immutable to level0 table.SSTable, if the number of
// immutable memtables is greater or equal to the MaximumMemtables.
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
					if err := storageState.forceFlushNextImmutableMemtable(); err != nil {
						slog.Error(fmt.Sprintf("could not flush memtable, error: %v", err))
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

// mayBeLoadExisting loads the existing StorageState from manifest.Manifest.
// It loads all the events.
// If the event is manifest.MemtableCreatedEventType -> it collects the id of the memtable.
// If the event is manifest.SSTableFlushedEventType -> it removes the id from the collection of memtable, stores the id in l0SSTableIds field.
// If the event is manifest.CompactionDoneEventType -> it creates StorageStateChangeEvent and applies it to the StorageState.
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

// recoverMemtables recovers all the immutable memtables identified by memtableIds from WAL.
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

// recoverL0SSTables recovers all the level0 SSTables.
// Loading an instance of table.SSTable is all about creating an in-memory representation of table.SSTable with a pointer to the
// actual file which contains the data.
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

// apply applies the StorageStateChangeEvent to the StorageState.
// It involves the following:
// 1) Getting an exclusive lock.
// 2) Setting the mapping between ssTableId and the corresponding ssTable.
// 3) Identifying all the ssTableIds to be removed.
// 4) Updating either l0SSTableIds or the level field.
// 5) Deleting the mapping from ssTables fields for the ssTableIds to be removed.
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

// orderedLevel0SSTableIds returns a slice of level0 SSTableIds from latest to the oldest level0 SSTable.
func (storageState *StorageState) orderedLevel0SSTableIds() []uint64 {
	ids := make([]uint64, 0, len(storageState.l0SSTableIds))
	for l0SSTableIndex := len(storageState.l0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
		ids = append(ids, storageState.l0SSTableIds[l0SSTableIndex])
	}
	return ids
}
