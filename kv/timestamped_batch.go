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

func NewTimestampedBatch() *TimestampedBatch {
	return &TimestampedBatch{}
}

func (batch *TimestampedBatch) Put(key Key, value Value) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, value, EntryKindPut})
	return batch
}

func (batch *TimestampedBatch) Delete(key Key) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, EmptyValue, EntryKindDelete})
	return batch
}

func (batch *TimestampedBatch) AllEntries() []Entry {
	return batch.entries
}

func (batch TimestampedBatch) SizeInBytes() int {
	size := 0
	for _, entry := range batch.entries {
		size += entry.SizeInBytes()
	}
	return size
}
