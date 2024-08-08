package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchWithASingleEntry(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	timestampedBatch := NewTimestampedBatchFrom(*batch, 10)
	assert.Equal(t, 1, len(timestampedBatch.AllEntries()))
}

func TestBatchWithTwoEntries(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	batch.Delete([]byte("consensus"))

	timestampedBatch := NewTimestampedBatchFrom(*batch, 5)
	assert.Equal(t, 2, len(timestampedBatch.AllEntries()))
}

func TestBatchWithThreeEntries(t *testing.T) {
	batch := NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))

	timestampedBatch := NewTimestampedBatchFrom(*batch, 5)
	assert.Equal(t, 21, timestampedBatch.SizeInBytes())
}
