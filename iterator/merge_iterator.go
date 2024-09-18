package iterator

import (
	"container/heap"
	"go-lsm/kv"
)

// An IndexedIteratorMinHeap is a min-heap of IndexedIterator.
type IndexedIteratorMinHeap []IndexedIterator

func (heap IndexedIteratorMinHeap) Len() int           { return len(heap) }
func (heap IndexedIteratorMinHeap) Less(i, j int) bool { return heap[i].IsPrioritizedOver(heap[j]) }
func (heap IndexedIteratorMinHeap) Swap(i, j int)      { heap[i], heap[j] = heap[j], heap[i] }
func (heap *IndexedIteratorMinHeap) Push(element any) {
	*heap = append(*heap, element.(IndexedIterator))
}
func (heap *IndexedIteratorMinHeap) Pop() any {
	old := *heap
	size := len(old)
	last := old[size-1]
	*heap = old[0 : size-1]
	return last
}

// IndexedIterator wraps the iterator with the index provided by the user.
type IndexedIterator struct {
	index int
	Iterator
}

// NewIndexedIterator creates a new instance of IndexedIterator.
func NewIndexedIterator(index int, iterator Iterator) IndexedIterator {
	return IndexedIterator{
		index:    index,
		Iterator: iterator,
	}
}

// IsPrioritizedOver returns true if the key referred by the indexedIterator is smaller than the key referred by the other.
// If the keys are the same, IndexedIterator with smaller index is prioritized.
func (indexedIterator IndexedIterator) IsPrioritizedOver(other IndexedIterator) bool {
	comparisonResult := indexedIterator.Key().CompareKeysWithDescendingTimestamp(other.Key())
	if comparisonResult == 0 {
		return indexedIterator.index < other.index
	}
	return comparisonResult < 0
}

// MergeIterator merges multiple iterators.
// Imagine a Scan operation with a few memtables: one current memtable and other immutable memtables.
// To scan over all these memtables, we can create multiple iterators, one for each memtable.
// However, we need a composite iterator which merges all these iterators. This is why MergeIterator comes in.
// All these memtables are individually sorted by keys, so MergeIterator can use a binary-heap to pick the iterator
// to return the keys in increasing order.
//
// Let's consider an example with 2 iterators, over two memtables (these could be SSTables also):
// iterator1: ("consensus", 6) -> ("raft"),  ("diskType", 7) -> ("etcd").
// iterator2: ("consensus", 7) -> ("paxos"), ("storage", 8) -> ("NVMe").
//
// The MergeIterator will return the keys in the following order:
// ("consensus", 7) -> ("paxos") | ("consensus", 6) -> ("raft") | ("diskType", 7) -> ("etcd") | ("storage", 8) -> ("NVMe")
// It does not eliminate same keys with multiple versions (/commit-timestamp).
// It is possible that multiple iterators may have the same key, in such a case, iterator with smaller index has the higher
// priority.
type MergeIterator struct {
	current   IndexedIterator
	iterators *IndexedIteratorMinHeap
}

// NewMergeIterator creates a new instance of MergeIterator.
func NewMergeIterator(iterators []Iterator) *MergeIterator {
	prioritizedIterators := &IndexedIteratorMinHeap{}
	heap.Init(prioritizedIterators)

	for index, iterator := range iterators {
		if iterator != nil && iterator.IsValid() {
			heap.Push(prioritizedIterators, NewIndexedIterator(index, iterator))
		}
	}
	//maintain a current iterator which is the first (smallest) element from the binary-heap.
	//each element of heap is an instance of IndexedIterator.
	if prioritizedIterators.Len() > 0 {
		return &MergeIterator{
			current:   heap.Pop(prioritizedIterators).(IndexedIterator),
			iterators: prioritizedIterators,
		}
	}
	return &MergeIterator{
		current: NewIndexedIterator(0, nothingIterator),
	}
}

// Key returns the key referred by the current iterator.
func (iterator *MergeIterator) Key() kv.Key {
	return iterator.current.Key()
}

// Value returns the value referred by the current iterator.
func (iterator *MergeIterator) Value() kv.Value {
	return iterator.current.Value()
}

// IsValid return true if the current iterator is valid.
func (iterator *MergeIterator) IsValid() bool {
	return iterator.current.IsValid()
}

// Next involves:
// 1). Advancing the other iterators on the same key.
// 2). Advancing the current iterator.
// 3). May be getting a new current iterator.
// 4). Maybe swapping the current iterator with the iterator from index 0 of binary-heap.
func (iterator *MergeIterator) Next() error {
	if err := iterator.advanceOtherIteratorsOnSameKey(); err != nil {
		return err
	}
	if err := iterator.advanceCurrent(); err != nil {
		return err
	}
	if iterator.maybePopNewCurrent() {
		return nil
	}
	return iterator.maybeSwapCurrent()
}

// Close closes all the iterators.
func (iterator *MergeIterator) Close() {
	iterator.current.Close()
	if iterator.iterators != nil && iterator.iterators.Len() > 0 {
		for _, anIterator := range *iterator.iterators {
			anIterator.Close()
		}
	}
}

// advanceOtherIteratorsOnSameKey advances the other iterators present in the binary-heap if the key is the same as
// that of current iterator.
func (iterator *MergeIterator) advanceOtherIteratorsOnSameKey() error {
	current := iterator.current
	for index, anIterator := range *iterator.iterators {
		if current.Key().IsEqualTo(anIterator.Key()) {
			if err := iterator.advance(anIterator); err != nil {
				heap.Pop(iterator.iterators).(IndexedIterator).Close()
				return err
			}
			if !anIterator.IsValid() {
				heap.Pop(iterator.iterators).(IndexedIterator).Close()
			} else {
				heap.Fix(iterator.iterators, index)
			}
		} else {
			break
		}
	}
	return nil
}

// maybePopNewCurrent maybe get a new iterator from the binary-heap if the current iterator becomes invalid.
func (iterator *MergeIterator) maybePopNewCurrent() bool {
	if !iterator.current.IsValid() {
		if iterator.iterators.Len() > 0 {
			iterator.current = heap.Pop(iterator.iterators).(IndexedIterator)
		}
		return true
	}
	return false
}

// maybeSwapCurrent swaps the current iterator with the iterator at index 0 of binary-heap.
// Consider the example:
// iterator[0]: ("consensus", 5) -> "paxos",      ("diskType", 5) -> "SSD"
// current    : ("accurate", 2) -> "consistency", ("consensus", 4) -> "raft"
// After the current iterator has returned the key "accurate", the next call will put the current iterator at the key "consensus" with
// timestamp 4. However, iterator at index 0 of binary-heap has the key "consensus" with timestamp 5.
// So, maybeSwapCurrent will perform a swap operation of the current iterator and the iterator at index 0 of binary-heap.
func (iterator *MergeIterator) maybeSwapCurrent() error {
	if iterator.iterators.Len() > 0 {
		current := iterator.current
		iterators := *iterator.iterators

		if !current.IsPrioritizedOver(iterators[0]) {
			current, iterators[0] = iterators[0], current
			iterator.current = current
			iterator.iterators = &iterators
		}
	}
	return nil
}

// advanceCurrent advances the current iterator
func (iterator *MergeIterator) advanceCurrent() error {
	return iterator.current.Next()
}

// advance advances the given iterator.
func (iterator *MergeIterator) advance(indexedIterator IndexedIterator) error {
	return indexedIterator.Next()
}
