package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchWithASingleEntry(t *testing.T) {
	batch := NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft"))
	assert.Equal(t, 1, len(batch.AllEntries()))
}

func TestBatchWithTwoEntries(t *testing.T) {
	batch := NewBatch().
		Put(NewStringKey("consensus"), NewStringValue("raft")).
		Delete(NewStringKey("consensus"))

	assert.Equal(t, 2, len(batch.AllEntries()))
}

func TestBatchWithThreeEntries(t *testing.T) {
	batch := NewBatch().Put(NewStringKey("consensus"), NewStringValue("raft"))
	assert.Equal(t, 13, batch.Size())
}
