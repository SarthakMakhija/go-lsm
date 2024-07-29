package memory

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

const testMemtableSize = 1 << 10

func TestEmptyMemtable(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	assert.True(t, memTable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestMemtableWithASingleKeyIncludingTimestampWhichReturnsTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestMemtableWithASingleKeyIncludingTimestampDoesNotReturnTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))

	_, ok := memTable.Get(txn.NewStringKeyWithTimestamp("consensus", 2))
	assert.False(t, ok)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := memTable.Get(txn.NewStringKey("storage"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("storage"), txn.NewStringValue("NVMe"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)

	value, ok = memTable.Get(txn.NewStringKey("storage"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("NVMe"), value)
}

func TestMemtableWithADelete(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Delete(txn.NewStringKey("consensus"))

	value, ok := memTable.Get(txn.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("epoch"), txn.NewStringKey("epoch")))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("distributed"), txn.NewStringKey("zen")))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive3(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("epoch"), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKey("distributed"), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKey("consensus"), txn.NewStringKey("distributed")))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusiveWithTimestamp1(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 1), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("epoch", 2), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("distributed", 3), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("distributed", 4), txn.NewStringKeyWithTimestamp("zen", 4)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusiveWithTimestamp2(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 1), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringValue("paxos"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("epoch", 2), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("distributed", 3), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringKeyWithTimestamp("distributed", 2)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("paxos"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusiveWithTimestamp3(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("consensus", 20), txn.NewStringValue("paxos"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("epoch", 20), txn.NewStringValue("time"))
	_ = memTable.Set(txn.NewStringKeyWithTimestamp("distributed", 2), txn.NewStringValue("Db"))

	iterator := memTable.Scan(txn.NewInclusiveKeyRange(txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringKeyWithTimestamp("distributed", 3)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, txn.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableAllEntries(t *testing.T) {
	memTable := NewMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	_ = memTable.Set(txn.NewStringKey("bolt"), txn.NewStringValue("kv"))
	_ = memTable.Set(txn.NewStringKey("etcd"), txn.NewStringValue("distributed"))

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
