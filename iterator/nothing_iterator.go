package iterator

import (
	"errors"
	"go-lsm/txn"
)

type NothingIterator struct{}

var errNoNextSupposedByNothingIterator = errors.New("no support for Next() by NothingIterator")

var nothingIterator = &NothingIterator{}

func (iterator NothingIterator) Key() txn.Key {
	return txn.EmptyKey
}

func (iterator *NothingIterator) Value() txn.Value {
	return txn.EmptyValue
}

func (iterator *NothingIterator) Next() error {
	return errNoNextSupposedByNothingIterator
}

func (iterator *NothingIterator) IsValid() bool {
	return false
}
