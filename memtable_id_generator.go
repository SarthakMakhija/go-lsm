package go_lsm

import "sync/atomic"

type MemtableIdGenerator struct {
	nextId atomic.Uint64
}

func NewMemtableIdGenerator() *MemtableIdGenerator {
	return &MemtableIdGenerator{}
}

func (generator *MemtableIdGenerator) NextId() uint64 {
	return generator.nextId.Add(1)
}
