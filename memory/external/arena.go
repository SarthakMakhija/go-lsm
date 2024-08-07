package external

import (
	"go-lsm/kv"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Arena should be lock-free.
type Arena struct {
	n   atomic.Uint32
	buf []byte
}

// newArena returns a new arena.
func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &Arena{buf: make([]byte, n)}
	out.n.Store(1)
	return out
}

func (arena *Arena) size() int64 {
	return int64(arena.n.Load())
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (arena *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := arena.n.Add(l)

	// Return the aligned offset.
	m := (n - l + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (arena *Arena) putVal(v kv.Value) uint32 {
	l := v.SizeAsUint32()
	n := arena.n.Add(l)

	m := n - l
	v.EncodeTo(arena.buf[m:])
	return m
}

func (arena *Arena) putKey(key kv.Key) uint32 {
	l := uint32(key.EncodedSizeInBytes())
	n := arena.n.Add(l)

	// m is the offset where you should write.
	// n = new len - key len give you the offset at which you should write.
	m := n - l
	// Copy to buffer from m:n
	copy(arena.buf[m:n], key.EncodedBytes())
	return m
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (arena *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(&arena.buf[offset]))
}

// getKey returns byte slice at offset.
func (arena *Arena) getKey(offset uint32, size uint16) kv.Key {
	return kv.DecodeFrom(arena.buf[offset : offset+uint32(size)])
}

// getValue returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (arena *Arena) getValue(offset uint32, size uint32) (ret kv.Value) {
	ret.DecodeFrom(arena.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (arena *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&arena.buf[0])))
}
