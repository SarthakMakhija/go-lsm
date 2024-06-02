package iterator

import (
	"go-lsm/txn"
)

type Iterator interface {
	Key() txn.Key
	Value() txn.Value
	Next() error
	IsValid() bool
}

type InclusiveBoundedIteratorType = *MergeIterator

type InclusiveBoundedIterator struct {
	inner           InclusiveBoundedIteratorType
	inclusiveEndKey txn.Key
	isValid         bool
}

func NewInclusiveBoundedIterator(iterator InclusiveBoundedIteratorType, inclusiveEndKey txn.Key) *InclusiveBoundedIterator {
	return &InclusiveBoundedIterator{
		inner:           iterator,
		inclusiveEndKey: inclusiveEndKey,
		isValid:         iterator.IsValid(),
	}
}

func (iterator *InclusiveBoundedIterator) Key() txn.Key {
	return iterator.inner.Key()
}

func (iterator *InclusiveBoundedIterator) Value() txn.Value {
	return iterator.inner.Value()
}

func (iterator *InclusiveBoundedIterator) Next() error {
	if err := iterator.inner.Next(); err != nil {
		return err
	}
	if !iterator.inner.IsValid() {
		iterator.isValid = false
		return nil
	}
	iterator.isValid = iterator.inner.Key().IsLessThanOrEqualTo(iterator.inclusiveEndKey)
	return iterator.moveToNonDeletedKey()
}

func (iterator *InclusiveBoundedIterator) IsValid() bool {
	return iterator.isValid
}

func (iterator *InclusiveBoundedIterator) moveToNonDeletedKey() error {
	for iterator.IsValid() && iterator.inner.Value().IsEmpty() {
		if err := iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}
