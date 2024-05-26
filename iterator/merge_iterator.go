package iterator

import (
	"container/heap"
	"go-lsm/txn"
)

// An MinHeapIndexedIterator is a min-heap of IndexedIterator.
type MinHeapIndexedIterator []IndexedIterator

func (heap MinHeapIndexedIterator) Len() int           { return len(heap) }
func (heap MinHeapIndexedIterator) Less(i, j int) bool { return heap[i].IsPrioritizedOver(heap[j]) }
func (heap MinHeapIndexedIterator) Swap(i, j int)      { heap[i], heap[j] = heap[j], heap[i] }
func (heap *MinHeapIndexedIterator) Push(element any) {
	*heap = append(*heap, element.(IndexedIterator))
}
func (heap *MinHeapIndexedIterator) Pop() any {
	old := *heap
	size := len(old)
	last := old[size-1]
	*heap = old[0 : size-1]
	return last
}

type IndexedIterator struct {
	index int
	Iterator
}

func NewIndexedIterator(index int, iterator Iterator) IndexedIterator {
	return IndexedIterator{
		index:    index,
		Iterator: iterator,
	}
}

func (indexedIterator IndexedIterator) IsPrioritizedOver(other IndexedIterator) bool {
	comparisonResult := indexedIterator.Key().Compare(other.Key())
	if comparisonResult == 0 {
		return indexedIterator.index < other.index
	}
	return comparisonResult < 0
}

type MergeIterator struct {
	current   IndexedIterator
	iterators *MinHeapIndexedIterator
}

func NewMergeIterator(iterators []Iterator) *MergeIterator {
	prioritizedIterators := &MinHeapIndexedIterator{}
	heap.Init(prioritizedIterators)

	for index, iterator := range iterators {
		if iterator.IsValid() {
			heap.Push(prioritizedIterators, NewIndexedIterator(index, iterator))
		}
	}
	return &MergeIterator{
		current:   heap.Pop(prioritizedIterators).(IndexedIterator),
		iterators: prioritizedIterators,
	}
}

func (iterator *MergeIterator) Key() txn.Key {
	return iterator.current.Key()
}

func (iterator *MergeIterator) Value() txn.Value {
	return iterator.current.Value()
}

func (iterator *MergeIterator) IsValid() bool {
	return iterator.current.IsValid()
}

func (iterator *MergeIterator) Next() error {
	current := iterator.current
	if err := iterator.advanceOtherIteratorsOnSameKey(current); err != nil {
		return err
	}
	if err := iterator.advance(current); err != nil {
		return err
	}
	if iterator.maybePopNew(current) {
		return nil
	}
	return iterator.maybeSwapCurrent(current)
}

func (iterator *MergeIterator) advanceOtherIteratorsOnSameKey(current IndexedIterator) error {
	for _, anIterator := range *iterator.iterators {
		if current.Key().IsEqualTo(anIterator.Key()) {
			if err := iterator.advance(anIterator); err != nil {
				heap.Pop(iterator.iterators)
				return err
			}
			if !anIterator.IsValid() {
				heap.Pop(iterator.iterators)
			}
		} else {
			break
		}
	}
	return nil
}

func (iterator *MergeIterator) maybePopNew(current IndexedIterator) bool {
	if !current.IsValid() {
		if iterator.iterators.Len() > 0 {
			iterator.current = heap.Pop(iterator.iterators).(IndexedIterator)
		}
		return true
	}
	return false
}

func (iterator *MergeIterator) maybeSwapCurrent(current IndexedIterator) error {
	if iterator.iterators.Len() > 0 {
		iterators := *iterator.iterators
		if !current.IsPrioritizedOver(iterators[0]) {
			current, iterators[0] = iterators[0], current
			iterator.current = current
			iterator.iterators = &iterators
		}
	}
	return nil
}

func (iterator *MergeIterator) advance(indexedIterator IndexedIterator) error {
	return indexedIterator.Next()
}
