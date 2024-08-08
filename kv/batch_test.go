package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyBatch(t *testing.T) {
	batch := NewBatch()
	assert.Equal(t, true, batch.IsEmpty())
}

func TestNonEmptyBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))
	assert.Equal(t, false, batch.IsEmpty())
}

func TestAddsDuplicateKeyInBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))
	err := batch.Put([]byte("HDD"), []byte("Hard disk"))

	assert.Error(t, err)
	assert.Equal(t, DuplicateKeyInBatchErr, err)
}

func TestGetTheValueOfAKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))

	value, ok := batch.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk", value.String())
}

func TestGetTheValueOfANonExistingKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))

	_, ok := batch.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestContainsTheKey(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("HDD"))
	assert.Equal(t, true, contains)
}

func TestDoesNotContainTheKey(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("SSD"))
	assert.Equal(t, false, contains)
}

func TestGetsTheTimestampedBatch(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("HDD"), []byte("Hard disk"))

	timestampedBatch := NewTimestampedBatchFrom(*batch, 1)
	assert.Equal(t, 1, len(timestampedBatch.AllEntries()))
	assert.Equal(t, uint64(1), timestampedBatch.AllEntries()[0].Timestamp())
	assert.Equal(t, "HDD", timestampedBatch.AllEntries()[0].RawString())
	assert.Equal(t, "Hard disk", timestampedBatch.AllEntries()[0].Value.String())
}
