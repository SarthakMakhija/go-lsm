package iterator

import (
	"errors"
	"go-lsm/kv"
)

// NothingIterator is a no-operation iterator.
// It is created from MergeIterator, if are of the iterators passed to the MergeIterator are invalid.
type NothingIterator struct{}

var errNoNextSupposedByNothingIterator = errors.New("no support for Next() by NothingIterator")

var nothingIterator = &NothingIterator{}

// Key returns kv.EmptyKey.
func (iterator NothingIterator) Key() kv.Key {
	return kv.EmptyKey
}

// Value returns kv.EmptyValue.
func (iterator *NothingIterator) Value() kv.Value {
	return kv.EmptyValue
}

// Next returns an error errNoNextSupposedByNothingIterator.
func (iterator *NothingIterator) Next() error {
	return errNoNextSupposedByNothingIterator
}

// IsValid returns false.
func (iterator *NothingIterator) IsValid() bool {
	return false
}

// Close does nothing.
func (iterator NothingIterator) Close() {}
