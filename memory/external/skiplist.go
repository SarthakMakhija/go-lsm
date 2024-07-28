package external

import (
	"go-lsm/txn"
	"math"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

// MaxNodeSize is the memory footprint of a node of maximum height.
const MaxNodeSize = int(unsafe.Sizeof(node{}))

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value atomic.Uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]atomic.Uint32
}

type SkipList struct {
	height  atomic.Int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	head    *node
	ref     atomic.Int32
	arena   *Arena
	OnClose func()
}

// NewSkipList makes a new empty skiplist, with a given arena size
func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	head := newNode(arena, txn.EmptyKey, txn.EmptyValue, maxHeight)
	s := &SkipList{head: head, arena: arena}
	s.height.Store(1)
	s.ref.Store(1)
	return s
}

// Put inserts the key-value pair.
func (skipList *SkipList) Put(key txn.Key, value txn.Value) {
	// Since we allow over-write, we may not need to create a new node. We might not even need to
	// increase the height. Let'skipList defer these actions.
	listHeight := skipList.getHeight()

	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node

	prev[listHeight] = skipList.head
	next[listHeight] = nil

	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = skipList.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].setValue(skipList.arena, value)
			return
		}
	}

	// We do need to create a new node.
	height := skipList.randomHeight()
	x := newNode(skipList.arena, key, value, height)

	// Try to increase skipList.height via CAS.
	listHeight = skipList.getHeight()
	for height > int(listHeight) {
		if skipList.height.CompareAndSwap(listHeight, int32(height)) {
			// Successfully increased skipList.height.
			break
		}
		listHeight = skipList.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				///y.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = skipList.findSpliceForLevel(key, skipList.head, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				//y.AssertTrue(prev[i] != next[i])
			}
			nextOffset := skipList.arena.getNodeOffset(next[i])
			x.tower[i].Store(nextOffset)
			if prev[i].casNextOffset(i, nextOffset, skipList.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = skipList.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				//y.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				prev[i].setValue(skipList.arena, value)
				return
			}
		}
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (skipList *SkipList) Get(key txn.Key) (txn.Value, bool) {
	foundNode, _ := skipList.findNear(key, false, true) // findGreaterOrEqual.
	if foundNode == nil {
		return txn.EmptyValue, false
	}

	nextKey := skipList.arena.getKey(foundNode.keyOffset, foundNode.keySize)
	if !key.IsEqualTo(nextKey) {
		return txn.EmptyValue, false
	}
	valOffset, valSize := foundNode.getValueOffset()
	return skipList.arena.getValue(valOffset, valSize), true
}

// Empty returns if the SkipList is empty.
func (skipList *SkipList) Empty() bool {
	return skipList.findLast() == nil
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (skipList *SkipList) NewIterator() *Iterator {
	skipList.incrRef()
	return &Iterator{list: skipList}
}

// MemSize returns the size of the SkipList in terms of how much memory is used within its internal
// arena.
func (skipList *SkipList) MemSize() int64 { return skipList.arena.size() }

// incrRef increases the refcount
func (skipList *SkipList) incrRef() {
	skipList.ref.Add(1)
}

// decrRef decrements the refcount, deallocating the SkipList when done using it
func (skipList *SkipList) decrRef() {
	newRef := skipList.ref.Add(-1)
	if newRef > 0 {
		return
	}
	if skipList.OnClose != nil {
		skipList.OnClose()
	}

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skipList when we are supposed to have no reference!
	skipList.arena = nil
	// Since the head references the arena'skipList buf, as long as the head is kept around
	// GC can't release the buf.
	skipList.head = nil
}

func newNode(arena *Arena, key txn.Key, v txn.Value, height int) *node {
	// The base level is already allocated in the node struct.
	offset := arena.putNode(height)
	node := arena.getNode(offset)
	node.keyOffset = arena.putKey(key)
	node.keySize = uint16(key.EncodedSizeInBytes())
	node.height = uint16(height)
	node.value.Store(encodeValue(arena.putVal(v), v.SizeAsUint32()))
	return node
}

func (node *node) getValueOffset() (uint32, uint32) {
	value := node.value.Load()
	return decodeValue(value)
}

func (node *node) key(arena *Arena) txn.Key {
	return arena.getKey(node.keyOffset, node.keySize)
}

func (node *node) setValue(arena *Arena, v txn.Value) {
	valOffset := arena.putVal(v)
	value := encodeValue(valOffset, v.SizeAsUint32())
	node.value.Store(value)
}

func (node *node) getNextOffset(h int) uint32 {
	return node.tower[h].Load()
}

func (node *node) casNextOffset(h int, old, val uint32) bool {
	return node.tower[h].CompareAndSwap(old, val)
}

func (skipList *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (skipList *SkipList) getNext(nd *node, height int) *node {
	return skipList.arena.getNode(nd.getNextOffset(height))
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (skipList *SkipList) findNear(key txn.Key, less bool, allowEqual bool) (*node, bool) {
	x := skipList.head
	level := int(skipList.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := skipList.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let'skipList return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == skipList.head {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(skipList.arena)
		cmp := key.Compare(nextKey)
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return skipList.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == skipList.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == skipList.head {
			return nil, false
		}
		return x, false
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (skipList *SkipList) findSpliceForLevel(key txn.Key, before *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := skipList.getNext(before, level)
		if next == nil {
			return before, next
		}
		nextKey := next.key(skipList.arena)
		cmp := key.Compare(nextKey)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

func (skipList *SkipList) getHeight() int32 {
	return skipList.height.Load()
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (skipList *SkipList) findLast() *node {
	n := skipList.head
	level := int(skipList.getHeight()) - 1
	for {
		next := skipList.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == skipList.head {
				return nil
			}
			return n
		}
		level--
	}
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *SkipList
	n    *node
}

// Close frees the resources held by the iterator
func (s *Iterator) Close() error {
	s.list.decrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() txn.Key {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *Iterator) Value() txn.Value {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getValue(valOffset, valSize)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *Iterator) ValueUint64() uint64 {
	return s.n.value.Load()
}

// Next advances to the next position.
func (s *Iterator) Next() {
	s.n = s.list.getNext(s.n, 0)
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target txn.Key) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.head, 0)
}

// FastRand is a fast thread local random function.
//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32
