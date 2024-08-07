package external

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestPutAndGetTheKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(kv.NewStringKeyWithTimestamp("consensus", 10), kv.NewStringValue("raft"))

	value, ok := skipList.Get(kv.NewStringKeyWithTimestamp("consensus", 10))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestPutAndGetTheKeyWithTimestampHigherThanThatOfTheKeyPresentInSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(kv.NewStringKeyWithTimestamp("consensus", 2), kv.NewStringValue("raft"))

	value, ok := skipList.Get(kv.NewStringKeyWithTimestamp("consensus", 3))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestGetANonExistingKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	value, ok := skipList.Get(kv.NewStringKeyWithTimestamp("consensus", 4))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestIterateOverSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	skipList.Put(kv.NewStringKeyWithTimestamp("bolt", 5), kv.NewStringValue("kv"))
	skipList.Put(kv.NewStringKeyWithTimestamp("badger", 6), kv.NewStringValue("LSM"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("badger", 6), iterator.Key())
	assert.Equal(t, kv.NewStringValue("LSM"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("bolt", 5), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 4), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.Valid())
}

func TestIterateOverSkipListHavingAKeyWithMultipleTimestamps(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(kv.NewStringKeyWithTimestamp("consensus", 4), kv.NewStringValue("raft"))
	skipList.Put(kv.NewStringKeyWithTimestamp("consensus", 5), kv.NewStringValue("paxos"))
	skipList.Put(kv.NewStringKeyWithTimestamp("bolt", 2), kv.NewStringValue("kv"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("bolt", 2), iterator.Key())
	assert.Equal(t, kv.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 5), iterator.Key())
	assert.Equal(t, kv.NewStringValue("paxos"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, kv.NewStringKeyWithTimestamp("consensus", 4), iterator.Key())
	assert.Equal(t, kv.NewStringValue("raft"), iterator.Value())

	iterator.Next()

	assert.False(t, iterator.Valid())
}
