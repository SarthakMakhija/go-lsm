package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchWithASingleEntry(t *testing.T) {
	batch := NewTimestampedBatch().Put(NewStringKeyWithTimestamp("consensus", 10), NewStringValue("raft"))
	assert.Equal(t, 1, len(batch.AllEntries()))
}

func TestBatchWithTwoEntries(t *testing.T) {
	batch := NewTimestampedBatch().
		Put(NewStringKeyWithTimestamp("consensus", 5), NewStringValue("raft")).
		Delete(NewStringKeyWithTimestamp("consensus", 5))

	assert.Equal(t, 2, len(batch.AllEntries()))
}

func TestBatchWithThreeEntries(t *testing.T) {
	batch := NewTimestampedBatch().Put(NewStringKeyWithTimestamp("consensus", 10), NewStringValue("raft"))
	assert.Equal(t, 21, batch.SizeInBytes())
}