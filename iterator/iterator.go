package iterator

import (
	"go-lsm/kv"
)

type Iterator interface {
	Key() kv.Key
	Value() kv.Value
	Next() error
	IsValid() bool
	Close()
}

type InclusiveBoundedIteratorType = *MergeIterator

type InclusiveBoundedIterator struct {
	inner           InclusiveBoundedIteratorType
	inclusiveEndKey kv.Key
	isValid         bool
	previousKey     kv.Key
}

func NewInclusiveBoundedIterator(iterator InclusiveBoundedIteratorType, inclusiveEndKey kv.Key) *InclusiveBoundedIterator {
	inclusiveBoundedIterator := &InclusiveBoundedIterator{
		inner:           iterator,
		inclusiveEndKey: inclusiveEndKey,
		isValid:         iterator.IsValid(),
	}
	if err := inclusiveBoundedIterator.keepLatestTimestamp(); err != nil {
		panic(err)
	}
	return inclusiveBoundedIterator
}

func (iterator *InclusiveBoundedIterator) Key() kv.Key {
	return iterator.inner.Key()
}

func (iterator *InclusiveBoundedIterator) Value() kv.Value {
	return iterator.inner.Value()
}

func (iterator *InclusiveBoundedIterator) Next() error {
	if err := iterator.advance(); err != nil {
		return err
	}
	return iterator.keepLatestTimestamp()
}

func (iterator *InclusiveBoundedIterator) IsValid() bool {
	return iterator.isValid
}

func (iterator *InclusiveBoundedIterator) Close() {
	iterator.inner.Close()
}

func (iterator *InclusiveBoundedIterator) keepLatestTimestamp() error {
	for {
		for iterator.inner.IsValid() && iterator.inner.Key().IsRawKeyEqualTo(iterator.previousKey) {
			if err := iterator.advance(); err != nil {
				return err
			}
		}
		if !iterator.inner.IsValid() {
			break
		}
		iterator.previousKey = iterator.inner.Key()
		for iterator.inner.IsValid() &&
			iterator.inner.Key().IsRawKeyEqualTo(iterator.previousKey) &&
			iterator.inner.Key().Timestamp() > iterator.inclusiveEndKey.Timestamp() {
			if err := iterator.advance(); err != nil {
				return err
			}
		}
		if !iterator.inner.IsValid() {
			break
		}
		if !iterator.inner.Key().IsRawKeyEqualTo(iterator.previousKey) {
			continue
		}
		if !iterator.inner.Value().IsEmpty() {
			break
		}
	}
	return nil
}

func (iterator *InclusiveBoundedIterator) advance() error {
	if err := iterator.inner.Next(); err != nil {
		return err
	}
	if !iterator.inner.IsValid() {
		iterator.isValid = false
		return nil
	}
	iterator.isValid = iterator.inner.Key().IsLessThanOrEqualTo(iterator.inclusiveEndKey)
	return nil
}
