package internal

import (
	"errors"
	"sync/atomic"
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
	keyLengthSize   = uint32(unsafe.Sizeof(uint16(0))) // 2 bytes: supports keys up to 65,535 bytes
	valueLengthSize = uint32(unsafe.Sizeof(uint32(0))) // 4 bytes: supports values up to 4 GB
	nextOffsetSize  = uint32(unsafe.Sizeof(uint32(0))) // 4 bytes: points to next node offset in arena

	// Relative Field Offsets within a single Node
	// [0..2)   -> Key Length
	// [2..6)   -> Value Length
	// [6..10)  -> Next Node Offset
	// [10..End)-> Key & Value Payload
	keyLengthOffset   = uint32(0)
	valueLengthOffset = keyLengthOffset + keyLengthSize     // Offset 2
	nextOffsetOffset  = valueLengthOffset + valueLengthSize // Offset 6

	nodeHeaderSize = keyLengthSize + valueLengthSize + nextOffsetSize // 10 bytes
)

var (
	// ErrArenaFull is returned when an allocation exceeds the fixed arena capacity.
	ErrArenaFull = errors.New("arena out of memory")
)

// Arena provides monotonic bump allocation within a fixed byte buffer.
type Arena struct {
	buffer     []byte
	nextOffset atomic.Uint32
}

// NewArena allocates the backing byte slice and reserves offset 0 for null.
func NewArena(size int64) *Arena {
	arena := &Arena{
		buffer: make([]byte, size),
	}
	arena.nextOffset.Store(1) // Offset 0 is reserved for nullOffset
	return arena
}

// allocate reserves a contiguous block of 'size' bytes and returns its starting nextOffset.
func (arena *Arena) allocate(size uint32) (uint32, error) {
	for {
		possibleNextOffset := arena.nextOffset.Load()
		if int64(possibleNextOffset+size) > int64(len(arena.buffer)) {
			return nullOffset, ErrArenaFull
		}
		if arena.nextOffset.CompareAndSwap(possibleNextOffset, possibleNextOffset+size) {
			return possibleNextOffset, nil
		}
	}
}

// bytes returns a slice referencing [offset, nextOffset+size] within the arena.
func (arena *Arena) bytes(offset, size uint32) []byte {
	if offset == nullOffset {
		return nil
	}
	return arena.buffer[offset : offset+size]
}

// MemSize returns the current memory allocated inside the arena in bytes.
func (arena *Arena) MemSize() int64 {
	return int64(arena.nextOffset.Load())
}
