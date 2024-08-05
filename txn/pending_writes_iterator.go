package txn

type PendingWritesIterator struct {
	batch      *Batch
	batchIndex int
	timestamp  uint64
}

// NewPendingWritesIterator TODO: Seek, Deleted keys, checking for range end
func NewPendingWritesIterator(batch *Batch, timestamp uint64) *PendingWritesIterator {
	return &PendingWritesIterator{
		batch:      batch.sortOnKeys(),
		batchIndex: 0,
		timestamp:  timestamp,
	}
}

func (iterator *PendingWritesIterator) Key() Key {
	pair := iterator.batch.getAtIndex(iterator.batchIndex)
	return NewKey(pair.key, iterator.timestamp)
}

func (iterator *PendingWritesIterator) Value() Value {
	return iterator.batch.getAtIndex(iterator.batchIndex).value
}

func (iterator *PendingWritesIterator) Next() error {
	iterator.batchIndex++
	return nil
}

func (iterator *PendingWritesIterator) IsValid() bool {
	return iterator.batchIndex < iterator.batch.Length()
}

func (iterator *PendingWritesIterator) Close() {}
