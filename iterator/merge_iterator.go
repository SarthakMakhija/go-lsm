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
	comparisonResult := indexedIterator.Key().CompareKeysWithDescendingTimestamp(other.Key())
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
		if iterator != nil && iterator.IsValid() {
			heap.Push(prioritizedIterators, NewIndexedIterator(index, iterator))
		}
	}
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

func (iterator *MergeIterator) Close() {
	iterator.current.Close()
	if iterator.iterators != nil && iterator.iterators.Len() > 0 {
		for _, anIterator := range *iterator.iterators {
			anIterator.Close()
		}
	}
}

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

func (iterator *MergeIterator) maybePopNewCurrent() bool {
	if !iterator.current.IsValid() {
		if iterator.iterators.Len() > 0 {
			iterator.current = heap.Pop(iterator.iterators).(IndexedIterator)
		}
		return true
	}
	return false
}

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

func (iterator *MergeIterator) advanceCurrent() error {
	return iterator.current.Next()
}

func (iterator *MergeIterator) advance(indexedIterator IndexedIterator) error {
	return indexedIterator.Next()
}
