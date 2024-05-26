package iterator

import "go-lsm"

type Iterator interface {
	Key() go_lsm.Key
	Value() go_lsm.Value
	Next() error
	IsValid() bool
}
