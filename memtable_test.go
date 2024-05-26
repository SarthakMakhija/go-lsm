package go_lsm

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestEmptyMemtable(t *testing.T) {
	memTable := NewMemtable(1)
	assert.True(t, memTable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("storage"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("storage"), txn.NewStringValue("NVMe"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}

func TestTheSizeOfMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	size := memTable.Size()
	assert.Equal(t, uint64(13), size)
}

func TestMemtableWithADelete(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Delete(txn.NewStringKey("consensus"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveRange(txn.NewStringKey("epoch"), txn.NewStringKey("epoch")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveRange(txn.NewStringKey("distributed"), txn.NewStringKey("zen")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive3(t *testing.T) {
	memTable := NewMemtable(1)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveRange(txn.NewStringKey("consensus"), txn.NewStringKey("distributed")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}
