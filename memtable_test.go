package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyMemtable(t *testing.T) {
	memTable := NewMemTable(1)
	assert.True(t, memTable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	value, ok := memTable.Get(NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, NewStringValue("raft"), value)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	value, ok := memTable.Get(NewStringKey("storage"))
	assert.False(t, ok)
	assert.Equal(t, emptyValue, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memTable.Set(NewStringKey("storage"), NewStringValue("NVMe"))

	value, ok := memTable.Get(NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, NewValue([]byte("raft")), value)

	value, ok = memTable.Get(NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, NewValue([]byte("NVMe")), value)
}

func TestTheSizeOfMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))

	size := memTable.Size()
	assert.Equal(t, uint64(13), size)
}

func TestMemtableWithADelete(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memTable.Delete(NewStringKey("consensus"))

	value, ok := memTable.Get(NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, emptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memTable.Set(NewStringKey("epoch"), NewStringValue("time"))
	memTable.Set(NewStringKey("distributed"), NewStringValue("Db"))

	iterator := memTable.ScanInclusive(NewStringKey("epoch"), NewStringKey("epoch"))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("time"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memTable := NewMemTable(1)
	memTable.Set(NewStringKey("consensus"), NewStringValue("raft"))
	memTable.Set(NewStringKey("epoch"), NewStringValue("time"))
	memTable.Set(NewStringKey("distributed"), NewStringValue("Db"))

	iterator := memTable.ScanInclusive(NewStringKey("distributed"), NewStringKey("zen"))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("Db"), iterator.Value())

	iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, NewStringValue("time"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.IsValid())
}
