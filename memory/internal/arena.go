package internal

import (
	"errors"
	"unsafe"
)

/**
Byte Offset:
0        1                                                                   Capacity
+--------+-------------------------------------------------+-----------------+
| 0x00   | Allocated Node Data                             | Unallocated     |
| (NULL) | [ Node 1 ]  [ Node 2 ]  [ Node 3 ] ...          | Free Buffer     |
+--------+-------------------------------------------------+-----------------+
         ^                                                 ^
      Offset 1                                       arena.offset (Next Allocation)
*/

const (
	// nullOffset denotes a nil pointer/offset in the arena.
	nullOffset uint32 = 0

	// Field Sizes in Bytes
	keyLengthSize  = uint32(unsafe.Sizeof(uint16(0))) // 2 bytes: supports keys up to 65,535 bytes
	valLengthSize  = uint32(unsafe.Sizeof(uint32(0))) // 4 bytes: supports values up to 4 GB
	nextOffsetSize = uint32(unsafe.Sizeof(uint32(0))) // 4 bytes: points to next node offset in arena

	// Relative Field Offsets within a single Node
	// [0..2)   -> Key Length
	// [2..6)   -> Value Length
	// [6..10)  -> Next Node Offset
	// [10..End)-> Key & Value Payload
	keyLenOffset     = uint32(0)
	valLenOffset     = keyLenOffset + keyLengthSize // Offset 2
	nextOffsetOffset = valLenOffset + valLengthSize // Offset 6

	nodeHeaderSize = keyLengthSize + valLengthSize + nextOffsetSize // 10 bytes
)

var (
	// ErrArenaFull is returned when an allocation exceeds the fixed arena capacity.
	ErrArenaFull = errors.New("arena out of memory")
)

// Arena provides monotonic bump allocation within a fixed byte buffer.
type Arena struct {
	buffer     []byte
	nextOffset uint32
}

// NewArena allocates the backing byte slice and reserves offset 0 for null.
func NewArena(size int64) *Arena {
	return &Arena{
		buffer:     make([]byte, size),
		nextOffset: 1, // Offset 0 is reserved for nullOffset
	}
}

// allocate reserves a contiguous block of 'size' bytes and returns its starting nextOffset.
func (arena *Arena) allocate(size uint32) (uint32, error) {
	if int64(arena.nextOffset+size) > int64(len(arena.buffer)) {
		return nullOffset, ErrArenaFull
	}
	offsetOfAllocation := arena.nextOffset
	arena.nextOffset += size
	return offsetOfAllocation, nil
}

// bytes returns a slice referencing [offset, nextOffset+size] within the arena.
func (arena *Arena) bytes(offset, size uint32) []byte {
	if offset == nullOffset {
		return nil
	}
	return arena.buffer[offset : offset+size]
}
