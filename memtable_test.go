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
	assert.Equal(t, emptyValue, value)
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

func TestMemtableWithADelete(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memtable.Delete(NewStringKey("consensus"))

	value, ok := memtable.Get(NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, emptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memtable.Set(NewStringKey("epoch"), NewStringValue("time"))
	memtable.Set(NewStringKey("distributed"), NewStringValue("Db"))

	iterator := memtable.ScanInclusive(NewStringKey("epoch"), NewStringKey("epoch"))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("time"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memtable := NewMemtable(1)
	memtable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memtable.Set(NewStringKey("epoch"), NewStringValue("time"))
	memtable.Set(NewStringKey("distributed"), NewStringValue("Db"))

	iterator := memtable.ScanInclusive(NewStringKey("distributed"), NewStringKey("zen"))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("Db"), iterator.Value())

	iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("time"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.IsValid())
}
