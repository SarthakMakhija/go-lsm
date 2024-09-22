package state

import (
	"sync"
)

// SSTableIdGenerator represents a mechanism to generate the SSTableId.
type SSTableIdGenerator struct {
	idLock sync.Mutex
	nextId uint64
}

// NewSSTableIdGenerator creates a new instance of SSTableIdGenerator.
func NewSSTableIdGenerator() *SSTableIdGenerator {
	return &SSTableIdGenerator{}
}

// NextId generates the next id. It uses sync.Mutex to generate next id.
func (generator *SSTableIdGenerator) NextId() uint64 {
	generator.idLock.Lock()
	defer generator.idLock.Unlock()

	generator.nextId = generator.nextId + 1
	return generator.nextId
}

// setIdIfGreaterThanExisting sets the nextId field to the given id, if the given id is greater than or equal to the nextId field.
func (generator *SSTableIdGenerator) setIdIfGreaterThanExisting(id uint64) {
	generator.idLock.Lock()
	defer generator.idLock.Unlock()

	if generator.nextId < id {
		generator.nextId = id
	}
}
