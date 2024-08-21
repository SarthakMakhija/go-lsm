package txn

import (
	"bytes"
	"go-lsm/kv"
	"sort"
)

type PendingWritesIterator struct {
	keyValuePairs     []kv.RawKeyValuePair
	index             int
	beginTimestamp    uint64
	inclusiveKeyRange kv.InclusiveKeyRange[kv.RawKey]
}

func NewPendingWritesIterator(batch *kv.Batch, beginTimestamp uint64, keyRange kv.InclusiveKeyRange[kv.RawKey]) *PendingWritesIterator {
	keyValuePairs := batch.CloneKeyValuePairs()
	sort.Slice(keyValuePairs, func(i, j int) bool {
		return bytes.Compare(keyValuePairs[i].Key(), keyValuePairs[j].Key()) < 0
	})
	iterator := &PendingWritesIterator{
		keyValuePairs:     keyValuePairs,
		index:             0,
		beginTimestamp:    beginTimestamp,
		inclusiveKeyRange: keyRange,
	}
	iterator.seek(keyRange.Start())
	return iterator
}

func (iterator *PendingWritesIterator) Key() kv.Key {
	pair := iterator.keyValuePairs[iterator.index]
	return kv.NewKey(pair.Key(), iterator.beginTimestamp)
}

func (iterator *PendingWritesIterator) Value() kv.Value {
	return iterator.keyValuePairs[iterator.index].Value()
}

func (iterator *PendingWritesIterator) Next() error {
	iterator.index++
	return nil
}

func (iterator *PendingWritesIterator) IsValid() bool {
	return iterator.index < len(iterator.keyValuePairs) &&
		kv.RawKey(iterator.Key().RawBytes()).IsLessThanOrEqualTo(iterator.inclusiveKeyRange.End())
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
