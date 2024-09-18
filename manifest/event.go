package manifest

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"go-lsm/compact/meta"
	"io"
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
	CompactionDoneEventType  uint8 = 2
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

// SSTableFlushed defines an SSTable flushed (to L0) event. (Memtable flushed to SSTable).
type SSTableFlushed struct {
	SsTableId uint64
}

// CompactionDone defines a compaction done event.
type CompactionDone struct {
	NewSSTableIds []uint64
	Description   meta.SimpleLeveledCompactionDescription
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
func decodeMemtableCreated(buffer []byte) (*MemtableCreated, int) {
	return NewMemtableCreated(binary.LittleEndian.Uint64(buffer[:])), int(idSize)
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
func decodeSSTableFlushed(buffer []byte) (*SSTableFlushed, int) {
	return NewSSTableFlushed(binary.LittleEndian.Uint64(buffer[:])), int(idSize)
}

// NewCompactionDone creates a new CompactionDone event.
func NewCompactionDone(newSSTableIds []uint64, description meta.SimpleLeveledCompactionDescription) *CompactionDone {
	return &CompactionDone{
		NewSSTableIds: newSSTableIds,
		Description:   description,
	}
}

// encode encodes CompactionDone to byte slice.
func (compactionDone *CompactionDone) encode() ([]byte, error) {
	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(compactionDone)
	if err != nil {
		return nil, err
	}
	buffered := buffer.Bytes()
	encoded := make([]byte, int(eventTypeSize)+len(buffered))
	encoded[0] = CompactionDoneEventType
	copy(encoded[1:], buffered)

	return encoded, nil
}

// EventType returns the event type CompactionDoneEventType.
func (compactionDone *CompactionDone) EventType() uint8 {
	return CompactionDoneEventType
}

// decodeCompactionDone decodes the CompactionDone event from the byte slice.
func decodeCompactionDone(buffer []byte) (*CompactionDone, int) {
	compactionDone := &CompactionDone{}
	reader := &byteCountingReader{reader: bytes.NewReader(buffer)}
	err := gob.NewDecoder(reader).Decode(compactionDone)
	if err != nil {
		return nil, 0
	}
	return compactionDone, int(reader.count)
}

// decodeEventsFrom decodes all the events from the Manifest file. The passed buffer is the whole file.
func decodeEventsFrom(buffer []byte) []Event {
	var events []Event
	for len(buffer) > 0 {
		eventType := buffer[0]
		switch eventType {
		case MemtableCreatedEventType:
			memtableCreated, n := decodeMemtableCreated(buffer[eventTypeSize:])
			events = append(events, memtableCreated)
			buffer = buffer[n+int(eventTypeSize):]
		case SSTableFlushedEventType:
			ssTableFlushed, n := decodeSSTableFlushed(buffer[eventTypeSize:])
			events = append(events, ssTableFlushed)
			buffer = buffer[n+int(eventTypeSize):]
		case CompactionDoneEventType:
			compactionDone, n := decodeCompactionDone(buffer[eventTypeSize:])
			events = append(events, compactionDone)
			buffer = buffer[n+int(eventTypeSize):]
		}
	}
	return events
}

// byteCountingReader counts the number of bytes read while encapsulates a reader.
// It is mainly used in decoding of CompactionDoneEventType.
type byteCountingReader struct {
	reader io.Reader
	count  int64
}

// Read reads from the given byte slice.
func (reader *byteCountingReader) Read(p []byte) (n int, err error) {
	n, err = reader.reader.Read(p)
	reader.count += int64(n)
	return
}
