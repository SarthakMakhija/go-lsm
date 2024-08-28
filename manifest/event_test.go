package manifest

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMemtableCreatedEventEncodeAndDecode(t *testing.T) {
	memtableCreated := NewMemtableCreated(10)
	buffer, _ := memtableCreated.encode()

	decoded := decodeMemtableCreated(buffer[1:])
	assert.Equal(t, uint64(10), decoded.memtableId)
}

func TestNewSSTableFlushedEventEncodeAndDecode(t *testing.T) {
	ssTableFlushed := NewSSTableFlushed(20)
	buffer, _ := ssTableFlushed.encode()

	decoded := decodeSSTableFlushed(buffer[1:])
	assert.Equal(t, uint64(20), decoded.ssTableId)
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
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).memtableId)
	assert.Equal(t, uint64(20), events[1].(*SSTableFlushed).ssTableId)
}
