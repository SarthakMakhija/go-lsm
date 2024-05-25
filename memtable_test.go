package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyMemtable(t *testing.T) {
	memtable := NewMemtable(1)
	assert.True(t, memtable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	value, ok := memtable.Get(NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("raft"), value)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	value, ok := memtable.Get(NewStringKey("storage"))
	assert.False(t, ok)
	assert.Equal(t, Value{}, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memtable.Set(NewStringKey("storage"), NewStringValue("NVMe"))

	value, ok := memtable.Get(NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, NewValue([]byte("raft")), value)

	value, ok = memtable.Get(NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, NewValue([]byte("NVMe")), value)
}

func TestTheSizeOfMemtableWithASingleKey(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	size := memtable.Size()
	assert.Equal(t, uint64(13), size)
}
