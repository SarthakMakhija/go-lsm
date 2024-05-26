package go_lsm

import (
	"github.com/huandu/skiplist"
	"go-lsm/txn"
	"sync/atomic"
)

type Memtable struct {
	id      uint
	size    atomic.Uint64
	entries *skiplist.SkipList
}

func NewMemtable(id uint) *Memtable {
	return &Memtable{
		id: id,
		entries: skiplist.New(skiplist.GreaterThanFunc(func(key, otherKey interface{}) int {
			left := key.(txn.Key)
			right := otherKey.(txn.Key)

			return left.Compare(right)
		})),
	}
}

func (memtable *Memtable) Get(key txn.Key) (txn.Value, bool) {
	value, ok := memtable.entries.GetValue(key)
	if !ok || value.(txn.Value).IsEmpty() {
		return txn.EmptyValue, false
	}
	return value.(txn.Value), true
}

func (memtable *Memtable) Set(key txn.Key, value txn.Value) {
	memtable.size.Add(uint64(key.Size() + value.Size()))
	memtable.entries.Set(key, value)
}

func (memtable *Memtable) Delete(key txn.Key) {
	memtable.Set(key, txn.EmptyValue)
}

func (memtable *Memtable) Scan(inclusiveRange txn.InclusiveRange) *MemtableIterator {
	return &MemtableIterator{
		element: memtable.entries.Find(inclusiveRange.Start()),
		endKey:  inclusiveRange.End(),
	}
}

func (memtable *Memtable) IsEmpty() bool {
	return memtable.entries.Len() == 0
}

func (memtable *Memtable) Size() uint64 {
	return memtable.size.Load()
}

type MemtableIterator struct {
	element *skiplist.Element
	endKey  txn.Key
}

func (iterator *MemtableIterator) Key() txn.Key {
	return iterator.element.Key().(txn.Key)
}

func (iterator *MemtableIterator) Value() txn.Value {
	return iterator.element.Value.(txn.Value)
}

func (iterator *MemtableIterator) Next() error {
	iterator.element = iterator.element.Next()
	return nil
}

func (iterator *MemtableIterator) IsValid() bool {
	return iterator.element != nil && iterator.element.Key().(txn.Key).IsLessThanOrEqualTo(iterator.endKey)
}
