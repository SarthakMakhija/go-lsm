package memory

import (
	"fmt"
	"go-lsm/kv"
	"go-lsm/log"
	"go-lsm/memory/external"
)

// WalPresence indicates the presence of WAL.
type WalPresence struct {
	EnableWAL        bool
	WALDirectoryPath string
}

// NewWALPresence creates a new instance of WalPresence.
func NewWALPresence(enableWAL bool, walDirectoryPath string) WalPresence {
	return WalPresence{
		EnableWAL:        enableWAL,
		WALDirectoryPath: walDirectoryPath,
	}
}

// Memtable is an in-memory data structure which holds versioned key kv.Key and kv.Value pairs.
// Memtable uses [Skiplist](https://tech-lessons.in/en/blog/serializable_snapshot_isolation/#skiplist-and-mvcc) as its storage
// data structure.
// The Skiplist (external.SkipList) is shamelessly take from [Badger](https://github.com/dgraph-io/badger).
// It is a lock-free implementation of Skiplist.
// It is important to have a lock-free implementation,
// otherwise scan operation will take lock(s) (/read-locks) and it will start interfering with write operations.
type Memtable struct {
	id                  uint64
	memTableSizeInBytes int64
	entries             *external.SkipList
	wal                 *log.WAL
}

// NewMemtable creates a new instance of Memtable with or without WAL.
func NewMemtable(id uint64, memTableSizeInBytes int64, walPresence WalPresence) *Memtable {
	if walPresence.EnableWAL {
		return newMemtableWithWAL(id, memTableSizeInBytes, walPresence.WALDirectoryPath)
	}
	return NewMemtableWithoutWAL(id, memTableSizeInBytes)
}

// NewMemtableWithoutWAL creates a new instance of Memtable without WAL.
func NewMemtableWithoutWAL(id uint64, memTableSizeInBytes int64) *Memtable {
	return &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
		wal:                 nil,
	}
}

// newMemtableWithWAL creates a new instance of Memtable with WAL.
func newMemtableWithWAL(id uint64, memTableSizeInBytes int64, walDirectoryPath string) *Memtable {
	wal, err := log.NewWALForId(id, walDirectoryPath)
	if err != nil {
		panic(fmt.Errorf("error creating new WAL: %v", err))
	}
	return &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
		wal:                 wal,
	}
}

// RecoverFromWAL recovers Memtable from WAL, it skips the check on memTableSizeInBytes.
// It returns the Memtable and the max timestamp, if there is no error in recovery.
func RecoverFromWAL(id uint64, memTableSizeInBytes int64, walDirectoryPath string) (*Memtable, uint64, error) {
	memtable := &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
	}
	var maxTimestamp uint64
	wal, err := log.Recover(log.CreateWalPathFor(id, walDirectoryPath), func(key kv.Key, value kv.Value) {
		memtable.entries.Put(key, value)
		maxTimestamp = max(maxTimestamp, key.Timestamp())
	})
	if err != nil {
		return nil, 0, err
	}
	memtable.wal = wal
	return memtable, maxTimestamp, nil
}

// Get returns the value for the key if found.
// It accepts a versioned key (kv.Key) and returns the key such that the commit-timestamp of the key <= begin-timestamp of the
// transaction.
func (memtable *Memtable) Get(key kv.Key) (kv.Value, bool) {
	value, ok := memtable.entries.Get(key)
	if !ok || value.IsEmpty() {
		return kv.EmptyValue, false
	}
	return value, true
}

// Set sets the key/value pair in the system. It involves the following:
// 1) Appending the key/value pair in the WAL, if WAL is present.
// 2) Writing the key/value pair in the Skiplist.
func (memtable *Memtable) Set(key kv.Key, value kv.Value) error {
	if memtable.wal != nil {
		if err := memtable.wal.Append(key, value); err != nil {
			return err
		}
	}
	memtable.entries.Put(key, value)
	return nil
}

// Delete is an append operation. It involves the following:
// 1) Appending the key/value pair in the WAL, if WAL is present.
// 2) Writing the key/value pair in the Skiplist.
func (memtable *Memtable) Delete(key kv.Key) error {
	return memtable.Set(key, kv.EmptyValue)
}

// Scan scans over the Memtable with the given inclusiveRange.
// It returns an iterator which seeks to a key that is greater than or equal to the start of the given key range.
// It goes until a key is less than or equal to the end key of the given key range.
// Scan takes care of the timestamp matching.
// This method will return a key with commit-timestamp <= begin-timestamp of the provided key, if the raw keys match.
// Let's take an example:
// Consider the following key/value pairs in the Memtable, here the numbers represent the commit-timestamp.
// ("consensus", 2)   -> "raft"
// ("epoch", 2)       -> "time"
// ("distributed", 3) -> "Db"
// Consider that the Scan operation involves the ("consensus", 2) -> ("distributed", 2) inclusive range.
// The numbers in the Scan operation represent the begin-timestamp.
// It will return an iterator that scans over ("consensus", "raft") key/value pair.
func (memtable *Memtable) Scan(inclusiveRange kv.InclusiveKeyRange[kv.Key]) *MemtableIterator {
	return NewMemtableIterator(memtable.entries.NewIterator(), inclusiveRange)
}

// AllEntries returns all the keys present in the memtable.
// If a key with multiple version is present, all the versions are returned.
func (memtable *Memtable) AllEntries(callback func(key kv.Key, value kv.Value)) {
	iterator := memtable.entries.NewIterator()
	defer func() {
		_ = iterator.Close()
	}()
	for iterator.SeekToFirst(); iterator.Valid(); iterator.Next() {
		callback(iterator.Key(), iterator.Value())
	}
}

// Sync performs a fsync operation on WAL.
func (memtable *Memtable) Sync() {
	if memtable.wal != nil {
		_ = memtable.wal.Sync()
	}
}

// DeleteWAL deletes the WAL (/WAL file).
func (memtable *Memtable) DeleteWAL() {
	if memtable.wal != nil {
		memtable.wal.DeleteFile()
	}
}

// IsEmpty returns true if the Memtable is empty.
func (memtable *Memtable) IsEmpty() bool {
	return memtable.entries.Empty()
}

// SizeInBytes returns the size of the Memtable.
func (memtable *Memtable) SizeInBytes() int64 {
	return memtable.entries.MemSize()
}

// CanFit returns true if the Memtable has the size enough for the requiredSizeInBytes.
func (memtable *Memtable) CanFit(requiredSizeInBytes int64) bool {
	return memtable.SizeInBytes()+requiredSizeInBytes+int64(external.MaxNodeSize) < memtable.memTableSizeInBytes
}

// Id returns the id of Memtable.
func (memtable *Memtable) Id() uint64 {
	return memtable.id
}

// WalPath returns the WAL path of the Memtable, if WAL is enabled, else returns blank.
func (memtable *Memtable) WalPath() (string, error) {
	if memtable.wal != nil {
		return memtable.wal.Path()
	}
	return "", nil
}

// MemtableIterator represents an iterator over Memtable.
// It is a wrapper over the iterator provided by external.SkipList.
type MemtableIterator struct {
	internalIterator *external.Iterator
	endKey           kv.Key
}

// NewMemtableIterator creates a new instance of MemtableIterator, seeks to the key start of the keyRange.
func NewMemtableIterator(internalIterator *external.Iterator, keyRange kv.InclusiveKeyRange[kv.Key]) *MemtableIterator {
	internalIterator.Seek(keyRange.Start())
	return &MemtableIterator{
		internalIterator: internalIterator,
		endKey:           keyRange.End(),
	}
}

// Key returns the kv.Key.
func (iterator *MemtableIterator) Key() kv.Key {
	return iterator.internalIterator.Key()
}

// Value returns the kv.Value.
func (iterator *MemtableIterator) Value() kv.Value {
	return iterator.internalIterator.Value()
}

// Next moves the iterator ahead.
func (iterator *MemtableIterator) Next() error {
	iterator.internalIterator.Next()
	return nil
}

// IsValid returns true if the external.Iterator is valid and key represented by internalIterator is lessThanOrEqualTo
// the end key of the keyRange.
// Please check IsLessThanOrEqualTo of kv.Key.
func (iterator *MemtableIterator) IsValid() bool {
	return iterator.internalIterator.Valid() && iterator.internalIterator.Key().IsLessThanOrEqualTo(iterator.endKey)
}

// Close closes the MemtableIterator.
func (iterator *MemtableIterator) Close() {
	_ = iterator.internalIterator.Close()
}
