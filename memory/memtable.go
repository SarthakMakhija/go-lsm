package memory

import (
	"fmt"
	"go-lsm/kv"
	"go-lsm/log"
	"go-lsm/memory/external"
)

type WalPresence struct {
	EnableWAL        bool
	WALDirectoryPath string
}

func NewWALPresence(enableWAL bool, walDirectoryPath string) WalPresence {
	return WalPresence{
		EnableWAL:        enableWAL,
		WALDirectoryPath: walDirectoryPath,
	}
}

type Memtable struct {
	id                  uint64
	memTableSizeInBytes int64
	entries             *external.SkipList
	wal                 *log.WAL
}

func NewMemtable(id uint64, memTableSizeInBytes int64, walPresence WalPresence) *Memtable {
	if walPresence.EnableWAL {
		return newMemtableWithWAL(id, memTableSizeInBytes, walPresence.WALDirectoryPath)
	}
	return NewMemtableWithoutWAL(id, memTableSizeInBytes)
}

func NewMemtableWithoutWAL(id uint64, memTableSizeInBytes int64) *Memtable {
	return &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
		wal:                 nil,
	}
}

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

// recoverFromWAL recovers Memtable from WAL, it skips the check on memTableSizeInBytes.
func recoverFromWAL(id uint64, memTableSizeInBytes int64, path string) (*Memtable, error) {
	memtable := &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
	}
	wal, err := log.Recover(path, func(key kv.Key, value kv.Value) {
		memtable.entries.Put(key, value)
	})
	if err != nil {
		return nil, err
	}
	memtable.wal = wal
	return memtable, nil
}

func (memtable *Memtable) Get(key kv.Key) (kv.Value, bool) {
	value, ok := memtable.entries.Get(key)
	if !ok || value.IsEmpty() {
		return kv.EmptyValue, false
	}
	return value, true
}

func (memtable *Memtable) Set(key kv.Key, value kv.Value) error {
	if memtable.wal != nil {
		if err := memtable.wal.Append(key, value); err != nil {
			return err
		}
	}
	memtable.entries.Put(key, value)
	return nil
}

func (memtable *Memtable) Delete(key kv.Key) error {
	return memtable.Set(key, kv.EmptyValue)
}

func (memtable *Memtable) Scan(inclusiveRange kv.InclusiveKeyRange[kv.Key]) *MemtableIterator {
	return NewMemtableIterator(memtable.entries.NewIterator(), inclusiveRange)
}

func (memtable *Memtable) AllEntries(callback func(key kv.Key, value kv.Value)) {
	iterator := memtable.entries.NewIterator()
	defer func() {
		_ = iterator.Close()
	}()
	for iterator.SeekToFirst(); iterator.Valid(); iterator.Next() {
		callback(iterator.Key(), iterator.Value())
	}
}

func (memtable *Memtable) Sync() {
	if memtable.wal != nil {
		_ = memtable.wal.Sync()
	}
}

func (memtable *Memtable) DeleteWAL() {
	if memtable.wal != nil {
		memtable.wal.DeleteFile()
	}
}

func (memtable *Memtable) IsEmpty() bool {
	return memtable.entries.Empty()
}

func (memtable *Memtable) SizeInBytes() int64 {
	return memtable.entries.MemSize()
}

func (memtable *Memtable) CanFit(requiredSizeInBytes int64) bool {
	return memtable.SizeInBytes()+requiredSizeInBytes+int64(external.MaxNodeSize) < memtable.memTableSizeInBytes
}

func (memtable *Memtable) Id() uint64 {
	return memtable.id
}

func (memtable *Memtable) WalPath() (string, error) {
	if memtable.wal != nil {
		return memtable.wal.Path()
	}
	return "", nil
}

type MemtableIterator struct {
	internalIterator *external.Iterator
	endKey           kv.Key
}

func NewMemtableIterator(internalIterator *external.Iterator, keyRange kv.InclusiveKeyRange[kv.Key]) *MemtableIterator {
	internalIterator.Seek(keyRange.Start())
	return &MemtableIterator{
		internalIterator: internalIterator,
		endKey:           keyRange.End(),
	}
}

func (iterator *MemtableIterator) Key() kv.Key {
	return iterator.internalIterator.Key()
}

func (iterator *MemtableIterator) Value() kv.Value {
	return iterator.internalIterator.Value()
}

func (iterator *MemtableIterator) Next() error {
	iterator.internalIterator.Next()
	return nil
}

func (iterator *MemtableIterator) IsValid() bool {
	return iterator.internalIterator.Valid() && iterator.internalIterator.Key().IsLessThanOrEqualTo(iterator.endKey)
}

func (iterator *MemtableIterator) Close() {
	_ = iterator.internalIterator.Close()
}
