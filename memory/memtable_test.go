package memory

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

const testMemtableSize = 1 << 10

func TestEmptyMemtable(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	assert.True(t, memTable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("storage"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("storage"), txn.NewStringValue("NVMe"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}

func TestMemtableWithADelete(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Delete(txn.NewStringKey("consensus"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("epoch"), txn.NewStringKey("epoch")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("distributed"), txn.NewStringKey("zen")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive3(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("consensus"), txn.NewStringKey("distributed")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableAllEntries(t *testing.T) {
	memTable := NewMemtable(1, testMemtableSize)
	memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	memTable.Set(txn.NewStringKey("bolt"), txn.NewStringValue("kv"))
	memTable.Set(txn.NewStringKey("etcd"), txn.NewStringValue("distributed"))

	var keys []txn.Key
	var values []txn.Value
	memTable.AllEntries(func(key txn.Key, value txn.Value) {
		keys = append(keys, key)
		values = append(values, value)
	})

	assert.Equal(t, []txn.Key{
		txn.NewStringKey("bolt"),
		txn.NewStringKey("consensus"),
		txn.NewStringKey("etcd"),
	}, keys)
	assert.Equal(t, []txn.Value{
		txn.NewStringValue("kv"),
		txn.NewStringValue("raft"),
		txn.NewStringValue("distributed"),
	}, values)
}
