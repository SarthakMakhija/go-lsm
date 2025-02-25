package memory

import (
	"go-lsm/kv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testMemtableSize = 1 << 10

func TestEmptyMemtable(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	assert.True(t, memTable.IsEmpty())
}

func TestMemtableWithASingleKey(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestMemtableWithASingleKeyIncludingTimestampWhichReturnsTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestMemtableWithASingleKeyIncludingTimestampDoesNotReturnTheValueOfTheKeyWithTimestampLessThanOrEqualToTheGiven(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))

	_, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 2))
	assert.False(t, ok)
}

func TestMemtableWithNonExistingKey(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("storage", 4))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestMemtableWithMultipleKeys(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("storage", 5), kv.NewStringValue("NVMe"))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)

	value, ok = memTable.Get(kv.NewStringKeyWithTimestamp("storage", 5))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("NVMe"), value)
}

func TestMemtableWithADelete(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Delete(kv.NewStringKeyWithTimestamp("consensus", 6))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestMemtableWithADeleteAndAGetWithTimestampHigherThanThatOfTheKeyInMemtable(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Delete(kv.NewStringKeyWithTimestamp("consensus", 6))

	value, ok := memTable.Get(kv.NewStringKeyWithTimestamp("consensus", 7))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestMemtableScanInclusive1(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("epoch", 6), kv.NewStringValue("time"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("Db"))

	iterator := memTable.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("epoch", 8), kv.NewStringKeyWithTimestamp("epoch", 8)))
	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive2(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("epoch", 6), kv.NewStringValue("time"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("Db"))

	iterator := memTable.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("distributed", 8), kv.NewStringKeyWithTimestamp("zen", 8)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("time"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive3(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("epoch", 6), kv.NewStringValue("time"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("distributed", 7), kv.NewStringValue("Db"))

	iterator := memTable.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 7), kv.NewStringKeyWithTimestamp("distributed", 7)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive4(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringValue("paxos"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("epoch", 2), kv.NewStringValue("time"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("distributed", 3), kv.NewStringValue("Db"))

	iterator := memTable.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringKeyWithTimestamp("distributed", 2)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("paxos"), iterator.Value())

	_ = iterator.Next()
	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableScanInclusive5(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 20), kv.NewStringValue("paxos"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("epoch", 20), kv.NewStringValue("time"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("distributed", 2), kv.NewStringValue("Db"))

	iterator := memTable.Scan(kv.NewInclusiveKeyRange(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringKeyWithTimestamp("distributed", 3)))
	defer iterator.Close()

	assert.True(t, iterator.IsValid())
	assert.Equal(t, kv.NewStringValue("Db"), iterator.Value())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestMemtableAllEntriesWithSameRawKeyWithDifferentTimestamps(t *testing.T) {
	memTable := newMemtableWithoutWAL(1, testMemtableSize)
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 1), kv.NewStringValue("raft"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringValue("paxos"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("bolt", 3), kv.NewStringValue("kv"))
	_ = memTable.Set(kv.NewStringKeyWithTimestamp("etcd", 4), kv.NewStringValue("distributed"))

	var keys []kv.Key
	var values []kv.Value
	memTable.AllEntries(func(key kv.Key, value kv.Value) {
		keys = append(keys, key)
		values = append(values, value)
	})

	assert.Equal(t, []kv.Key{
		kv.NewStringKeyWithTimestamp("bolt", 3),
		kv.NewStringKeyWithTimestamp("consensus", 2),
		kv.NewStringKeyWithTimestamp("consensus", 1),
		kv.NewStringKeyWithTimestamp("etcd", 4),
	}, keys)

	assert.Equal(t, []kv.Value{
		kv.NewStringValue("kv"),
		kv.NewStringValue("paxos"),
		kv.NewStringValue("raft"),
		kv.NewStringValue("distributed"),
	}, values)
}
