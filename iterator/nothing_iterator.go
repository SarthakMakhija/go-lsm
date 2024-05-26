package iterator

import (
	"errors"
	"go-lsm/txn"
)

type NothingIterator struct{}

var errNoNextSupposedByNothingIterator = errors.New("no next supposed by nothing iterator")

var nothingIterator = &NothingIterator{}

func (iterator NothingIterator) Key() txn.Key {
	return txn.NewKey(nil)
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
