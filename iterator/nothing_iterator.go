package iterator

import (
	"errors"
	"go-lsm/kv"
)

type NothingIterator struct{}

var errNoNextSupposedByNothingIterator = errors.New("no support for Next() by NothingIterator")

var nothingIterator = &NothingIterator{}

func (iterator NothingIterator) Key() kv.Key {
	return kv.EmptyKey
}

func (iterator *NothingIterator) Value() kv.Value {
	return kv.EmptyValue
}

func (iterator *NothingIterator) Next() error {
	return errNoNextSupposedByNothingIterator
}

func (iterator *NothingIterator) IsValid() bool {
	return false
}

func (iterator NothingIterator) Close() {}
