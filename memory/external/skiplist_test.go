package external

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestPutAndGetTheKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 10), txn.NewStringValue("raft"))

	value, ok := skipList.Get(txn.NewStringKeyWithTimestamp("consensus", 10))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestPutAndGetTheKeyWithTimestampHigherThanThatOfTheKeyPresentInSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringValue("raft"))

	value, ok := skipList.Get(txn.NewStringKeyWithTimestamp("consensus", 3))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestGetANonExistingKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	value, ok := skipList.Get(txn.NewStringKeyWithTimestamp("consensus", 4))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestIterateOverSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))
	skipList.Put(txn.NewStringKeyWithTimestamp("bolt", 5), txn.NewStringValue("kv"))
	skipList.Put(txn.NewStringKeyWithTimestamp("badger", 6), txn.NewStringValue("LSM"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("badger", 6), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("bolt", 5), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 4), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.Valid())
}

func TestIterateOverSkipListHavingAKeyWithMultipleTimestamps(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 4), txn.NewStringValue("raft"))
	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 5), txn.NewStringValue("paxos"))
	skipList.Put(txn.NewStringKeyWithTimestamp("bolt", 2), txn.NewStringValue("kv"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("bolt", 2), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 5), iterator.Key())
	assert.Equal(t, txn.NewStringValue("paxos"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKeyWithTimestamp("consensus", 4), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	iterator.Next()

	assert.False(t, iterator.Valid())
}
