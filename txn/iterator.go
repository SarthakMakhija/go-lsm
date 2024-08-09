package txn

import (
	"go-lsm/iterator"
	"go-lsm/kv"
)

type Iterator struct {
	transaction *Transaction
	inner       *iterator.MergeIterator
}

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

func (iterator *Iterator) Key() kv.Key {
	return iterator.inner.Key()
}

func (iterator *Iterator) Value() kv.Value {
	return iterator.inner.Value()
}

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

func (iterator *Iterator) IsValid() bool {
	return iterator.inner.IsValid()
}

func (iterator *Iterator) Close() {
	iterator.inner.Close()
}

func (iterator *Iterator) ignoreDeleted() error {
	for iterator.IsValid() && iterator.Value().IsEmpty() {
		if err := iterator.inner.Next(); err != nil {
			return err
		}
	}
	return nil
}
