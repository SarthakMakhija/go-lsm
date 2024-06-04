package go_lsm

import "sync/atomic"

type SSTableIdGenerator struct {
	nextId atomic.Uint64
}

func NewSSTableIdGenerator() *SSTableIdGenerator {
	return &SSTableIdGenerator{}
}

func (generator *SSTableIdGenerator) NextId() uint64 {
	return generator.nextId.Add(1)
}
