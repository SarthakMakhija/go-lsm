package go_lsm

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
	batch.entries = append(batch.entries, Entry{key, emptyValue, EntryKindDelete})
	return batch
}

func (batch *Batch) AllEntries() []Entry {
	return batch.entries
}
