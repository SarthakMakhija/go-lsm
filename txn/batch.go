package txn

type EntryKind int

const (
	EntryKindPut    = 1
	EntryKindDelete = 2
)

type Entry struct {
	Key
	Value
	EntryKind
}

func (entry Entry) IsKindPut() bool {
	return entry.EntryKind == EntryKindPut
}

func (entry Entry) IsKindDelete() bool {
	return entry.EntryKind == EntryKindDelete
}

func (entry Entry) SizeInBytes() int {
	return entry.Key.EncodedSizeInBytes() + entry.Value.SizeInBytes()
}

// Batch TODO: What if the batch has a get
type Batch struct {
	entries []Entry
}

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Put(key Key, value Value) *Batch {
	batch.entries = append(batch.entries, Entry{key, value, EntryKindPut})
	return batch
}

func (batch *Batch) Delete(key Key) *Batch {
	batch.entries = append(batch.entries, Entry{key, EmptyValue, EntryKindDelete})
	return batch
}

func (batch *Batch) AllEntries() []Entry {
	return batch.entries
}

func (batch Batch) SizeInBytes() int {
	size := 0
	for _, entry := range batch.entries {
		size += entry.SizeInBytes()
	}
	return size
}
