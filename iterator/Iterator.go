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
