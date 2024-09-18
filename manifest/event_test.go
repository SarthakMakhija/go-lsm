package manifest

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/compact/meta"
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

func TestNewCompactionDoneEventEncodeAndDecode(t *testing.T) {
	upperLevel := -1
	lowerLevel := 1
	upperLevelSSTableIds := []uint64{20, 30}
	lowerLevelSSTableIds := []uint64{50, 60}
	compactionDone := NewCompactionDone([]uint64{10, 14}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           upperLevel,
		LowerLevel:           lowerLevel,
		UpperLevelSSTableIds: upperLevelSSTableIds,
		LowerLevelSSTableIds: lowerLevelSSTableIds,
	})
	buffer, _ := compactionDone.encode()

	decoded, _ := decodeCompactionDone(buffer[1:])
	assert.Equal(t, []uint64{10, 14}, decoded.NewSSTableIds)
	assert.Equal(t, -1, decoded.Description.UpperLevel)
	assert.Equal(t, 1, decoded.Description.LowerLevel)
	assert.Equal(t, []uint64{20, 30}, decoded.Description.UpperLevelSSTableIds)
	assert.Equal(t, []uint64{50, 60}, decoded.Description.LowerLevelSSTableIds)
}

func TestNewCompactionDoneEventType(t *testing.T) {
	upperLevel := -1
	lowerLevel := 1
	upperLevelSSTableIds := []uint64{20, 30}
	lowerLevelSSTableIds := []uint64{50, 60}

	compactionDone := NewCompactionDone([]uint64{10, 14}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           upperLevel,
		LowerLevel:           lowerLevel,
		UpperLevelSSTableIds: upperLevelSSTableIds,
		LowerLevelSSTableIds: lowerLevelSSTableIds,
	})
	assert.Equal(t, CompactionDoneEventType, compactionDone.EventType())
}

func TestDecodeNewMemtableCreatedAndSSTableEventFlushedEvents(t *testing.T) {
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

func TestDecodeNewMemtableCreatedAndCompactionDoneEvents(t *testing.T) {
	memtableCreated := NewMemtableCreated(10)
	upperLevel := -1
	lowerLevel := 1
	upperLevelSSTableIds := []uint64{20, 30}
	lowerLevelSSTableIds := []uint64{50, 60}
	compactionDone := NewCompactionDone([]uint64{10, 11}, meta.SimpleLeveledCompactionDescription{
		UpperLevel:           upperLevel,
		LowerLevel:           lowerLevel,
		UpperLevelSSTableIds: upperLevelSSTableIds,
		LowerLevelSSTableIds: lowerLevelSSTableIds,
	})

	memtableCreatedBuffer, _ := memtableCreated.encode()
	compactionDoneBuffer, _ := compactionDone.encode()

	var buffer []byte
	buffer = append(buffer, memtableCreatedBuffer...)
	buffer = append(buffer, compactionDoneBuffer...)

	events := decodeEventsFrom(buffer)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).MemtableId)
	assert.Equal(t, []uint64{10, 11}, events[1].(*CompactionDone).NewSSTableIds)
}
