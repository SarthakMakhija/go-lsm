package manifest

import (
	"encoding/binary"
	"unsafe"
)

const (
	idSize        = unsafe.Sizeof(uint64(0))
	eventTypeSize = unsafe.Sizeof(uint8(0))
)

// Event types.
const (
	MemtableCreatedEventType uint8 = iota
	SSTableFlushedEventType  uint8 = 1
)

// Event represents a manifest event.
type Event interface {
	encode() ([]byte, error)
	EventType() uint8
}

// MemtableCreated defines a new memtable event.
type MemtableCreated struct {
	MemtableId uint64
}

// SSTableFlushed defines an SSTable flushed event. (Memtable flushed to SSTable).
type SSTableFlushed struct {
	SsTableId uint64
}

// NewMemtableCreated creates a new MemtableCreated event.
func NewMemtableCreated(memtableId uint64) *MemtableCreated {
	return &MemtableCreated{MemtableId: memtableId}
}

// encode encodes MemtableCreated to byte slice.
/*
 -----------------------------------------------
| 1 byte event type | 8 bytes for the MemtableId |
 -----------------------------------------------
*/
func (memtableCreated *MemtableCreated) encode() ([]byte, error) {
	buffer := make([]byte, eventTypeSize+idSize)
	buffer[0] = MemtableCreatedEventType
	binary.LittleEndian.PutUint64(buffer[1:], memtableCreated.MemtableId)
	return buffer, nil
}

// EventType returns the event type MemtableCreatedEventType.
func (memtableCreated *MemtableCreated) EventType() uint8 {
	return MemtableCreatedEventType
}

// decodeMemtableCreated decodes the MemtableCreated event from the byte slice.
// The buffer is a slice containing MemtableId.
func decodeMemtableCreated(buffer []byte) *MemtableCreated {
	return NewMemtableCreated(binary.LittleEndian.Uint64(buffer[:]))
}

// NewSSTableFlushed creates a new SSTableFlushed event.
func NewSSTableFlushed(ssTableId uint64) *SSTableFlushed {
	return &SSTableFlushed{SsTableId: ssTableId}
}

// encode encodes SSTableFlushed to byte slice.
/*
 -----------------------------------------------
| 1 byte event type | 8 bytes for the SsTableId |
 -----------------------------------------------
*/
func (ssTableFlushed *SSTableFlushed) encode() ([]byte, error) {
	buffer := make([]byte, eventTypeSize+idSize)
	buffer[0] = SSTableFlushedEventType
	binary.LittleEndian.PutUint64(buffer[1:], ssTableFlushed.SsTableId)
	return buffer, nil
}

// EventType returns the event type SSTableFlushedEventType.
func (ssTableFlushed *SSTableFlushed) EventType() uint8 {
	return SSTableFlushedEventType
}

// decodeSSTableFlushed decodes the SSTableFlushed event from the byte slice.
// The buffer is a slice containing SsTableId.
func decodeSSTableFlushed(buffer []byte) *SSTableFlushed {
	return NewSSTableFlushed(binary.LittleEndian.Uint64(buffer[:]))
}

// decodeEventsFrom decodes all the events from the Manifest file. The passed buffer is the whole file.
func decodeEventsFrom(buffer []byte) []Event {
	var events []Event
	for len(buffer) > 0 {
		eventType := buffer[0]
		switch eventType {
		case MemtableCreatedEventType:
			memtableCreated := decodeMemtableCreated(buffer[eventTypeSize:])
			events = append(events, memtableCreated)
			buffer = buffer[idSize+eventTypeSize:]
		case SSTableFlushedEventType:
			ssTableFlushed := decodeSSTableFlushed(buffer[eventTypeSize:])
			events = append(events, ssTableFlushed)
			buffer = buffer[idSize+eventTypeSize:]
		}
	}
	return events
}
