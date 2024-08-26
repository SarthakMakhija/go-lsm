package txn

import (
	"go-lsm/iterator"
	"go-lsm/kv"
)

// Iterator represents a readwrite transaction iterator.
// It holds an instance of iterator.MergeIterator which is created from:
// - PendingWritesIterator, and
// - iterator from state.StorageState
// The main reasons for creating this iterator include:
// 1) Skipping the deleted keys
// 2) Tracking reads for a readwrite transaction.
type Iterator struct {
	transaction *Transaction
	inner       *iterator.MergeIterator
}

// NewTransactionIterator creates a new instance of Iterator for transaction.
func NewTransactionIterator(transaction *Transaction, inner *iterator.MergeIterator) (*Iterator, error) {
	transactionIterator := &Iterator{transaction: transaction, inner: inner}
	if err := transactionIterator.ignoreDeleted(); err != nil {
		return nil, err
	}
	if transactionIterator.IsValid() {
		transactionIterator.transaction.trackReads(transactionIterator.Key().RawBytes())
	}
	return transactionIterator, nil
}

// Key returns the kv.Key.
func (iterator *Iterator) Key() kv.Key {
	return iterator.inner.Key()
}

// Value returns the kv.Value.
func (iterator *Iterator) Value() kv.Value {
	return iterator.inner.Value()
}

// Next involves the following:
// 1) Moves the merge iterator forward.
// 2) Ignores deleted keys.
// 3) Tracks key reads.
func (iterator *Iterator) Next() error {
	if err := iterator.inner.Next(); err != nil {
		return err
	}
	if err := iterator.ignoreDeleted(); err != nil {
		return err
	}
	if iterator.IsValid() {
		iterator.transaction.trackReads(iterator.Key().RawBytes())
	}
	return nil
}

// IsValid returns true if the iterator is valid.
func (iterator *Iterator) IsValid() bool {
	return iterator.inner.IsValid()
}

// Close closes the iterator.
func (iterator *Iterator) Close() {
	iterator.inner.Close()
}

// ignoreDeleted keeps moving the MergeIterator forward till the iterator is valid and the key is deleted.
func (iterator *Iterator) ignoreDeleted() error {
	for iterator.IsValid() && iterator.Value().IsEmpty() {
		if err := iterator.inner.Next(); err != nil {
			return err
		}
	}
	return nil
}
