package txn

import (
	"bytes"
	"sort"
)

type PendingWritesIterator struct {
	keyValuePairs []KeyValuePair
	index         int
	timestamp     uint64
}

// NewPendingWritesIterator TODO: Seek, Deleted keys, checking for range end
func NewPendingWritesIterator(batch *Batch, timestamp uint64) *PendingWritesIterator {
	keyValuePairs := make([]KeyValuePair, 0, len(batch.pairs))
	for _, pair := range batch.pairs {
		keyValuePairs = append(keyValuePairs, pair)
	}
	sort.Slice(keyValuePairs, func(i, j int) bool {
		return bytes.Compare(keyValuePairs[i].key, keyValuePairs[j].key) < 0
	})
	return &PendingWritesIterator{
		keyValuePairs: keyValuePairs,
		index:         0,
		timestamp:     timestamp,
	}
}

func (iterator *PendingWritesIterator) Key() Key {
	pair := iterator.keyValuePairs[iterator.index]
	return NewKey(pair.key, iterator.timestamp)
}

func (iterator *PendingWritesIterator) Value() Value {
	return iterator.keyValuePairs[iterator.index].value
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
		switch bytes.Compare(keyValuePair.key, key) {
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
