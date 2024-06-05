package memory

import (
	"go-lsm/memory/external"
	"go-lsm/txn"
)

type Memtable struct {
	id                  uint64
	memTableSizeInBytes int64
	entries             *external.SkipList
}

func NewMemtable(id uint64, memTableSizeInBytes int64) *Memtable {
	return &Memtable{
		id:                  id,
		memTableSizeInBytes: memTableSizeInBytes,
		entries:             external.NewSkipList(memTableSizeInBytes),
	}
}

func (memtable *Memtable) Get(key txn.Key) (txn.Value, bool) {
	value, ok := memtable.entries.Get(key)
	if !ok || value.IsEmpty() {
		return txn.EmptyValue, false
	}
	return value, true
}

func (memtable *Memtable) Set(key txn.Key, value txn.Value) {
	memtable.entries.Put(key, value)
}

func (memtable *Memtable) Delete(key txn.Key) {
	memtable.Set(key, txn.EmptyValue)
}

func (memtable *Memtable) Scan(inclusiveRange txn.InclusiveKeyRange) *MemtableIterator {
	return NewMemtableIterator(memtable.entries.NewIterator(), inclusiveRange)
}

func (memtable *Memtable) AllEntries(callback func(key txn.Key, value txn.Value)) {
	iterator := memtable.entries.NewIterator()
	defer func() {
		_ = iterator.Close()
	}()
	for iterator.SeekToFirst(); iterator.Valid(); iterator.Next() {
		callback(iterator.Key(), iterator.Value())
	}
}

func (memtable *Memtable) IsEmpty() bool {
	return memtable.entries.Empty()
}

func (memtable *Memtable) Size() int64 {
	return memtable.entries.MemSize()
}

func (memtable *Memtable) CanFit(requiredSize int64) bool {
	return memtable.Size()+requiredSize+int64(external.MaxNodeSize) < memtable.memTableSizeInBytes
}

func (memtable *Memtable) Id() uint64 {
	return memtable.id
}

type MemtableIterator struct {
	internalIterator *external.Iterator
	endKey           txn.Key
}

func NewMemtableIterator(internalIterator *external.Iterator, keyRange txn.InclusiveKeyRange) *MemtableIterator {
	internalIterator.Seek(keyRange.Start())
	return &MemtableIterator{
		internalIterator: internalIterator,
		endKey:           keyRange.End(),
	}
}

func (iterator *MemtableIterator) Key() txn.Key {
	return iterator.internalIterator.Key()
}

func (iterator *MemtableIterator) Value() txn.Value {
	return iterator.internalIterator.Value()
}

func (iterator *MemtableIterator) Next() error {
	iterator.internalIterator.Next()
	return nil
}

func (iterator *MemtableIterator) IsValid() bool {
	return iterator.internalIterator.Valid() && iterator.internalIterator.Key().IsLessThanOrEqualTo(iterator.endKey)
}
