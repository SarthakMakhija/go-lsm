package iterator

import (
	"go-lsm/kv"
)

// Iterator represents a common interface for all the iterators available in the system.
type Iterator interface {
	Key() kv.Key
	Value() kv.Value
	Next() error
	IsValid() bool
	Close()
}

type InclusiveBoundedIteratorType = *MergeIterator

// InclusiveBoundedIterator is the final iterator encapsulating MergeIterator, and is used for scanning with kv.InclusiveKeyRange.
// It serves the following:
// 1) Returns only the latest version (/timestamp) of a key, hence it tracks the previous key.
// 2) Ensures that the iterator does not go beyond the end key of the range.
type InclusiveBoundedIterator struct {
	inner           InclusiveBoundedIteratorType
	inclusiveEndKey kv.Key
	isValid         bool
	previousKey     kv.Key
}

// NewInclusiveBoundedIterator creates a new instance of InclusiveBoundedIterator.
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

// Key returns kv.Key.
func (iterator *InclusiveBoundedIterator) Key() kv.Key {
	return iterator.inner.Key()
}

// Value returns kv.Value.
func (iterator *InclusiveBoundedIterator) Value() kv.Value {
	return iterator.inner.Value()
}

// Next advances the iterator and keeps the latest timestamp of a key.
func (iterator *InclusiveBoundedIterator) Next() error {
	if err := iterator.advance(); err != nil {
		return err
	}
	return iterator.keepLatestTimestamp()
}

// IsValid returns true if the key referred to by the iterator is less than or equal to the end key of the range.
func (iterator *InclusiveBoundedIterator) IsValid() bool {
	return iterator.isValid
}

// Close closes the inner iterator.
func (iterator *InclusiveBoundedIterator) Close() {
	iterator.inner.Close()
}

// keepLatestTimestamp keeps the latest timestamp of a key.
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

// advance advances the iterator ahead and also sets isValid.
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
