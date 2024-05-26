package go_lsm

import (
	"github.com/huandu/skiplist"
	"go-lsm/txn"
	"sync/atomic"
)

type MemTable struct {
	id      uint
	size    atomic.Uint64
	entries *skiplist.SkipList
}

func NewMemTable(id uint) *MemTable {
	return &MemTable{
		id: id,
		entries: skiplist.New(skiplist.GreaterThanFunc(func(key, otherKey interface{}) int {
			left := key.(txn.Key)
			right := otherKey.(txn.Key)

			return left.Compare(right)
		})),
	}
}

func (memTable *MemTable) Get(key txn.Key) (txn.Value, bool) {
	value, ok := memTable.entries.GetValue(key)
	if !ok || value.(txn.Value).IsEmpty() {
		return txn.EmptyValue, false
	}
	return value.(txn.Value), true
}

func (memTable *MemTable) Set(key txn.Key, value txn.Value) {
	memTable.size.Add(uint64(key.Size() + value.Size()))
	memTable.entries.Set(key, value)
}

func (memTable *MemTable) Delete(key txn.Key) {
	memTable.Set(key, txn.EmptyValue)
}

func (memTable *MemTable) Scan(inclusiveRange txn.InclusiveRange) *MemTableIterator {
	return &MemTableIterator{
		element: memTable.entries.Find(inclusiveRange.Start()),
		endKey:  inclusiveRange.End(),
	}
}

func (memTable *MemTable) IsEmpty() bool {
	return memTable.entries.Len() == 0
}

func (memTable *MemTable) Size() uint64 {
	return memTable.size.Load()
}

type MemTableIterator struct {
	element *skiplist.Element
	endKey  txn.Key
}

func (iterator *MemTableIterator) Key() txn.Key {
	return iterator.element.Key().(txn.Key)
}

func (iterator *MemTableIterator) Value() txn.Value {
	return iterator.element.Value.(txn.Value)
}

func (iterator *MemTableIterator) Next() error {
	iterator.element = iterator.element.Next()
	return nil
}

func (iterator *MemTableIterator) IsValid() bool {
	return iterator.element != nil && iterator.element.Key().(txn.Key).IsLessThanOrEqualTo(iterator.endKey)
}
