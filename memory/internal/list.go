package internal

import "go-lsm/kv"

// SortedList is a singly linked list stored inside an Arena.
// Keys are ordered by raw key ascending, then by commit timestamp descending (MVCC ordering).
type SortedList struct {
	arena    *Arena
	headNode Node
}

// NewSortedList initializes a new list backed by an Arena of the given capacity.
func NewSortedList(capacity int64) *SortedList {
	arena := NewArena(capacity)
	return &SortedList{
		arena:    arena,
		headNode: nodeAt(arena, nullOffset),
	}
}

// Put inserts a versioned key-value pair into the sorted linked list.
func (list *SortedList) Put(key kv.Key, value kv.Value) error {
	node, err := newNode(list.arena, key, value)
	if err != nil {
		return err
	}

	// Case A: List is empty or new key belongs before current head
	if list.headNode.IsNull() || key.CompareKeysWithDescendingTimestamp(list.headNode.Key()) <= 0 {
		node.SetNext(list.headNode)
		list.headNode = node
		return nil
	}

	// Case B: Traverse the list to find the correct sorted insertion point
	curr := list.headNode
	for {
		next := curr.Next()
		if next.IsNull() {
			// Reached end of the list; append here
			curr.SetNext(node)
			break
		}
		if key.CompareKeysWithDescendingTimestamp(next.Key()) <= 0 {
			// Found slot: splice new node between curr and next
			node.SetNext(next)
			curr.SetNext(node)
			break
		}
		curr = next
	}

	return nil
}

// Get returns the value matching the raw key whose commit timestamp <= searchKey.Timestamp().
func (list *SortedList) Get(searchKey kv.Key) (kv.Value, bool) {
	curr := list.headNode

	for !curr.IsNull() {
		nodeKey := curr.Key()

		if searchKey.IsRawKeyEqualTo(nodeKey) {
			// Because identical raw keys are ordered by timestamp descending,
			// the first entry with commit timestamp <= search timestamp is visible.
			if nodeKey.Timestamp() <= searchKey.Timestamp() {
				return curr.Value(), true
			}
		} else if nodeKey.IsRawKeyGreaterThan(searchKey) {
			// Node raw key is strictly greater than search key; key does not exist.
			break
		}

		curr = curr.Next()
	}

	return kv.EmptyValue, false
}

// MemSize returns the current memory allocated inside the arena in bytes.
func (list *SortedList) MemSize() int64 {
	return int64(list.arena.nextOffset)
}

// Empty returns true if the list contains no elements.
func (list *SortedList) Empty() bool {
	return list.headNode.IsNull()
}

// NewIterator creates a new iterator starting at the head node.
func (list *SortedList) NewIterator() *SortedListIterator {
	return &SortedListIterator{
		list: list,
		curr: list.headNode,
	}
}

// NodeHeaderSize returns the header size of the node
func (list *SortedList) NodeHeaderSize() uint32 {
	return nodeHeaderSize
}
