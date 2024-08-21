package kv

type Kind int

const (
	EntryKindPut    = 1
	EntryKindDelete = 2
)

// Entry represents a Key, Value pair along with Kind.
type Entry struct {
	Key
	Value
	Kind
}

// IsKindPut returns true if the Entry is of kind EntryKindPut.
func (entry Entry) IsKindPut() bool {
	return entry.Kind == EntryKindPut
}

// IsKindDelete returns true if the Entry is of kind EntryKindDelete.
func (entry Entry) IsKindDelete() bool {
	return entry.Kind == EntryKindDelete
}

// SizeInBytes returns the size of the entry.
func (entry Entry) SizeInBytes() int {
	return entry.Key.EncodedSizeInBytes() + entry.Value.SizeInBytes()
}

// TimestampedBatch is a collection of Entry.
// Each Entry contains a Key, a Value and a Kind.
// An instance of Batch is converted to TimestampedBatch when the transaction (read/write) is ready to commit.
// An instance of TimestampedBatch represents entries containing keys with commit timestamp of the transaction.
type TimestampedBatch struct {
	entries []Entry
}

// NewTimestampedBatchFrom creates a new instance of TimestampedBatch from Batch and commitTimestamp of the transaction.
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

// AllEntries returns all the entries.
func (batch TimestampedBatch) AllEntries() []Entry {
	return batch.entries
}

// SizeInBytes returns the total size of TimestampedBatch.
func (batch TimestampedBatch) SizeInBytes() int {
	size := 0
	for _, entry := range batch.entries {
		size += entry.SizeInBytes()
	}
	return size
}

// put puts the Key, Value pair in the TimestampedBatch.
func (batch *TimestampedBatch) put(key Key, value Value) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, value, EntryKindPut})
	return batch
}

// delete is modeled as an append operation. It results in another Entry in TimestampedBatch with kind as EntryKindDelete.
func (batch *TimestampedBatch) delete(key Key) *TimestampedBatch {
	batch.entries = append(batch.entries, Entry{key, EmptyValue, EntryKindDelete})
	return batch
}
