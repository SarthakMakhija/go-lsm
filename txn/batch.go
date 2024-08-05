package txn

import (
	"bytes"
	"errors"
	"sort"
)

type KeyValuePair struct {
	key   []byte
	value Value
	kind  Kind
}

var DuplicateKeyInBatchErr = errors.New("batch already contains the key")

type Batch struct {
	pairs []KeyValuePair
}

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Put(key, value []byte) error {
	if batch.Contains(key) {
		return DuplicateKeyInBatchErr
	}
	batch.pairs = append(batch.pairs, KeyValuePair{
		key:   key,
		value: NewValue(value),
		kind:  EntryKindPut,
	})
	return nil
}

func (batch *Batch) Delete(key []byte) {
	batch.pairs = append(batch.pairs, KeyValuePair{
		key:   key,
		value: EmptyValue,
		kind:  EntryKindDelete,
	})
}

func (batch *Batch) Get(key []byte) (Value, bool) {
	for _, pair := range batch.pairs {
		if bytes.Compare(pair.key, key) == 0 {
			return pair.value, true
		}
	}
	return EmptyValue, false
}

func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

func (batch *Batch) Length() int {
	return len(batch.pairs)
}

func (batch *Batch) ToTimestampedBatch(commitTimestamp uint64) *TimestampedBatch {
	timestampedBatch := NewTimestampedBatch()
	for _, pair := range batch.pairs {
		if pair.kind == EntryKindPut {
			timestampedBatch.Put(NewKey(pair.key, commitTimestamp), pair.value)
		} else if pair.kind == EntryKindDelete {
			timestampedBatch.Delete(NewKey(pair.key, commitTimestamp))
		} else {
			panic("unsupported entry kind while converting the Batch to TimestampedBatch")
		}
	}
	return timestampedBatch
}

func (batch *Batch) getAtIndex(index int) KeyValuePair {
	return batch.pairs[index]
}

func (batch *Batch) sortOnKeys() *Batch {
	sort.Slice(batch.pairs, func(i, j int) bool {
		return bytes.Compare(batch.pairs[i].key, batch.pairs[j].key) < 0
	})
	return batch
}
