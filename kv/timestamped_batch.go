package kv

type Kind int

const (
	EntryKindPut    = 1
	EntryKindDelete = 2
)

type Entry struct {
	Key
	Value
	Kind
}

func (entry Entry) IsKindPut() bool {
	return entry.Kind == EntryKindPut
}

func (entry Entry) IsKindDelete() bool {
	return entry.Kind == EntryKindDelete
}

func (entry Entry) SizeInBytes() int {
	return entry.Key.EncodedSizeInBytes() + entry.Value.SizeInBytes()
}

type TimestampedBatch struct {
	entries []Entry
}

func NewTimestampedBatchFrom(batch Batch, commitTimestamp uint64) TimestampedBatch {
	timestampedBatch := &TimestampedBatch{}
	for _, pair := range batch.pairs {
		if pair.kind == EntryKindPut {
			timestampedBatch.put(NewKey(pair.key, commitTimestamp), pair.value)
		} else if pair.kind == EntryKindDelete {
			timestampedBatch.delete(NewKey(pair.key, commitTimestamp))
		} else {
			panic("unsupported entry kind while converting the Batch to TimestampedBatch")
		}
	}
	return *timestampedBatch
}

func (batch TimestampedBatch) AllEntries() []Entry {
	return batch.entries
}

func (batch TimestampedBatch) SizeInBytes() int {
	size := 0
	for _, entry := range batch.entries {
		size += entry.SizeInBytes()
	}
	return size
}

func (batch *TimestampedBatch) put(key Key, value Value) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, value, EntryKindPut})
	return batch
}

func (batch *TimestampedBatch) delete(key Key) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, EmptyValue, EntryKindDelete})
	return batch
}
