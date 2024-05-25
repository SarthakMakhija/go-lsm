package go_lsm

type Iterator interface {
	Key() Key
	Value() Value
	Next() error
	IsValid() bool
}
