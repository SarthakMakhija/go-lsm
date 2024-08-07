package txn

import (
	"bytes"
	"go-lsm/kv"
	"sort"
)

type PendingWritesIterator struct {
	keyValuePairs []kv.KeyValuePair
	index         int
	timestamp     uint64
}

// NewPendingWritesIterator TODO: Seek, Deleted keys, checking for range end
func NewPendingWritesIterator(batch *kv.Batch, timestamp uint64) *PendingWritesIterator {
	keyValuePairs := batch.CloneKeyValuePairs()
	sort.Slice(keyValuePairs, func(i, j int) bool {
		return bytes.Compare(keyValuePairs[i].Key(), keyValuePairs[j].Key()) < 0
	})
	return &PendingWritesIterator{
		keyValuePairs: keyValuePairs,
		index:         0,
		timestamp:     timestamp,
	}
}

func (iterator *PendingWritesIterator) Key() kv.Key {
	pair := iterator.keyValuePairs[iterator.index]
	return kv.NewKey(pair.Key(), iterator.timestamp)
}

func (iterator *PendingWritesIterator) Value() kv.Value {
	return iterator.keyValuePairs[iterator.index].Value()
}

func (iterator *PendingWritesIterator) Next() error {
	iterator.index++
	return nil
}

func (iterator *PendingWritesIterator) IsValid() bool {
	return iterator.index < len(iterator.keyValuePairs)
}

func (iterator *PendingWritesIterator) Close() {}

func (iterator *PendingWritesIterator) seek(key []byte) {
	iterator.index = len(iterator.keyValuePairs)
	low, high := 0, len(iterator.keyValuePairs)-1

	for low <= high {
		mid := low + (high-low)/2
		keyValuePair := iterator.keyValuePairs[mid]
		switch bytes.Compare(keyValuePair.Key(), key) {
		case -1:
			low = mid + 1
		case 0:
			iterator.index = mid
			return
		case 1:
			iterator.index = mid //possible index
			high = mid - 1
		}
	}
	return
}
