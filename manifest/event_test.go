package manifest

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMemtableCreatedEventEncodeAndDecode(t *testing.T) {
	memtableCreated := NewMemtableCreated(10)
	buffer, _ := memtableCreated.encode()

	decoded, _ := decodeMemtableCreated(buffer[1:])
	assert.Equal(t, uint64(10), decoded.MemtableId)
}

func TestNewMemtableCreatedEventType(t *testing.T) {
	memtableCreated := NewMemtableCreated(10)
	assert.Equal(t, MemtableCreatedEventType, memtableCreated.EventType())
}

func TestNewSSTableFlushedEventEncodeAndDecode(t *testing.T) {
	ssTableFlushed := NewSSTableFlushed(20)
	buffer, _ := ssTableFlushed.encode()

	decoded, _ := decodeSSTableFlushed(buffer[1:])
	assert.Equal(t, uint64(20), decoded.SsTableId)
}

func TestNewSSTableFlushedEventType(t *testing.T) {
	ssTableFlushed := NewSSTableFlushed(10)
	assert.Equal(t, SSTableFlushedEventType, ssTableFlushed.EventType())
}

func TestNewDecodeNewMemtableCreatedAndSSTableEventFlushedEvents(t *testing.T) {
	memtableCreated := NewMemtableCreated(10)
	ssTableFlushed := NewSSTableFlushed(20)

	memtableCreatedBuffer, _ := memtableCreated.encode()
	ssTableFlushedBuffer, _ := ssTableFlushed.encode()

	var buffer []byte
	buffer = append(buffer, memtableCreatedBuffer...)
	buffer = append(buffer, ssTableFlushedBuffer...)

	events := decodeEventsFrom(buffer)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).MemtableId)
	assert.Equal(t, uint64(20), events[1].(*SSTableFlushed).SsTableId)
}
