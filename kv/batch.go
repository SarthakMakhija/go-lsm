package kv

import (
	"bytes"
	"errors"
)

// RawKeyValuePair represents the key/value pair with Kind.
type RawKeyValuePair struct {
	key   []byte
	value Value
	kind  Kind
}

// Key returns the key.
func (kv RawKeyValuePair) Key() []byte {
	return kv.key
}

// Value returns the value.
func (kv RawKeyValuePair) Value() Value {
	return kv.value
}

var DuplicateKeyInBatchErr = errors.New("batch already contains the key")

// Batch is a collection of RawKeyValuePair.
// Batch is typically used in a transaction (txn.Transaction). All the inserts within a transaction (read/write transaction)
// are batched and finally the entire Batch is committed.
type Batch struct {
	pairs []RawKeyValuePair
}

// NewBatch creates an empty Batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Put puts the key/value pair in Batch.
// Returns DuplicateKeyInBatchErr if the key is already present in the Batch.
func (batch *Batch) Put(key, value []byte) error {
	if batch.Contains(key) {
		return DuplicateKeyInBatchErr
	}
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: NewValue(value),
		kind:  EntryKindPut,
	})
	return nil
}

// Delete is modeled as an append operation. It results in another RawKeyValuePair in batch with kind as EntryKindDelete.
func (batch *Batch) Delete(key []byte) {
	batch.pairs = append(batch.pairs, RawKeyValuePair{
		key:   key,
		value: EmptyValue,
		kind:  EntryKindDelete,
	})
}

// Get returns the Value for the given key if found.
func (batch *Batch) Get(key []byte) (Value, bool) {
	for _, pair := range batch.pairs {
		if bytes.Compare(pair.key, key) == 0 {
			return pair.value, true
		}
	}
	return EmptyValue, false
}

// Contains returns true of the key is present in Batch.
func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

// IsEmpty returns true if the Batch is empty.
func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

// Length returns the number of RawKeyValuePair(s) in the Batch.
func (batch *Batch) Length() int {
	return len(batch.pairs)
}

// CloneKeyValuePairs clones the RawKeyValuePair(s) present in the Batch.
func (batch *Batch) CloneKeyValuePairs() []RawKeyValuePair {
	keyValuePairs := make([]RawKeyValuePair, 0, batch.Length())
	for _, pair := range batch.pairs {
		keyValuePairs = append(keyValuePairs, pair)
	}
	return keyValuePairs
}
