package manifest

import (
	"encoding/binary"
	"unsafe"
)

const uint64Size = unsafe.Sizeof(uint64(0))

// Event represents a manifest event.
type Event interface {
	serialize() ([]byte, error)
}

// MemtableCreated defines a new memtable event.
type MemtableCreated struct {
	memtableId uint64
}

// SSTableFlushed defines an SSTable flushed event. (Memtable flushed to SSTable).
type SSTableFlushed struct {
	ssTableId uint64
}

// NewMemtableCreated creates a new MemtableCreated event.
func NewMemtableCreated(memtableId uint64) *MemtableCreated {
	return &MemtableCreated{memtableId: memtableId}
}

// serialize serializes MemtableCreated to byte slice.
func (memtableCreated *MemtableCreated) serialize() ([]byte, error) {
	buffer := make([]byte, uint64Size)
	binary.LittleEndian.PutUint64(buffer, memtableCreated.memtableId)
	return buffer, nil
}

// NewSSTableFlushed creates a new SSTableFlushed event.
func NewSSTableFlushed(ssTableId uint64) *SSTableFlushed {
	return &SSTableFlushed{ssTableId: ssTableId}
}

// serialize serializes SSTableFlushed to byte slice.
func (ssTableFlushed *SSTableFlushed) serialize() ([]byte, error) {
	buffer := make([]byte, uint64Size)
	binary.LittleEndian.PutUint64(buffer, ssTableFlushed.ssTableId)
	return buffer, nil
}
