package internal

import "go-lsm/kv"

// SortedListIterator provides sequential scanning over the SortedList.
type SortedListIterator struct {
	list *SortedList
	curr Node
}

// Seek advances the iterator to the first key >= targetKey in MVCC order.
func (iterator *SortedListIterator) Seek(targetKey kv.Key) {
	iterator.curr = iterator.list.headNode
	for !iterator.curr.IsNull() {
		currKey := iterator.curr.Key()
		if targetKey.CompareKeysWithDescendingTimestamp(currKey) <= 0 {
			break
		}
		iterator.curr = iterator.curr.Next()
	}
}

// SeekToFirst resets the iterator to the beginning of the list.
func (iterator *SortedListIterator) SeekToFirst() {
	iterator.curr = iterator.list.headNode
}

// Valid returns true if the iterator points to a valid node.
func (iterator *SortedListIterator) Valid() bool {
	return !iterator.curr.IsNull()
}

// Next advances the iterator to the next node.
func (iterator *SortedListIterator) Next() {
	if !iterator.curr.IsNull() {
		iterator.curr = iterator.curr.Next()
	}
}

// Key returns the versioned kv.Key at the current iterator position.
func (iterator *SortedListIterator) Key() kv.Key {
	return iterator.curr.Key()
}

// Value returns the kv.Value at the current iterator position.
func (iterator *SortedListIterator) Value() kv.Value {
	return iterator.curr.Value()
}

// Close closes the iterator.
func (iterator *SortedListIterator) Close() error {
	//no-operation close.
	return nil
}
