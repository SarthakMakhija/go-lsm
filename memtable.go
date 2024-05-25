package go_lsm

import (
	"bytes"
	"github.com/huandu/skiplist"
	"sync/atomic"
)

type Key struct {
	key []byte
}

func NewKey(key []byte) Key {
	return Key{key: key}
}

func NewStringKey(key string) Key {
	return Key{key: []byte(key)}
}

type Value struct {
	value []byte
}

var emptyValue = Value{value: nil}

func NewValue(value []byte) Value {
	return Value{value: value}
}

func NewStringValue(value string) Value {
	return Value{value: []byte(value)}
}

func (value Value) IsEmpty() bool {
	return value.value == nil
}

type MemTable struct {
	id      uint
	size    atomic.Uint64
	entries *skiplist.SkipList
}

func NewMemtable(id uint) *MemTable {
	return &MemTable{
		id: id,
		entries: skiplist.New(skiplist.GreaterThanFunc(func(key, otherKey interface{}) int {
			left := key.(Key)
			right := otherKey.(Key)

			return bytes.Compare(left.key, right.key)
		})),
	}
}

func (memTable *MemTable) Get(key Key) (Value, bool) {
	value, ok := memTable.entries.GetValue(key)
	if !ok || value.(Value).IsEmpty() {
		return emptyValue, false
	}
	return value.(Value), true
}

func (memTable *MemTable) Set(key Key, value Value) {
	size := len(key.key) + len(value.value)
	memTable.size.Add(uint64(size))
	memTable.entries.Set(key, value)
}

func (memTable *MemTable) Delete(key Key) {
	memTable.Set(key, emptyValue)
}

func (memTable *MemTable) IsEmpty() bool {
	return memTable.entries.Len() == 0
}

func (memTable *MemTable) Size() uint64 {
	return memTable.size.Load()
}
