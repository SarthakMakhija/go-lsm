package state

import (
	"sync"
)

type SSTableIdGenerator struct {
	idLock sync.Mutex
	nextId uint64
}

func NewSSTableIdGenerator() *SSTableIdGenerator {
	return &SSTableIdGenerator{}
}

func (generator *SSTableIdGenerator) setIdIfGreaterThanExisting(id uint64) {
	generator.idLock.Lock()
	defer generator.idLock.Unlock()

	if generator.nextId < id {
		generator.nextId = id
	}
}

func (generator *SSTableIdGenerator) NextId() uint64 {
	generator.idLock.Lock()
	defer generator.idLock.Unlock()

	generator.nextId = generator.nextId + 1
	return generator.nextId
}
