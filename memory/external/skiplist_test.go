package external

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestPutAndGetTheKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))

	value, ok := skipList.Get(txn.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestPutAndGetTheKeyWithTimestamp(t *testing.T) {
	skipList := NewSkipList(1 << 10)
	skipList.Put(txn.NewStringKeyWithTimestamp("consensus", 2), txn.NewStringValue("raft"))

	value, ok := skipList.Get(txn.NewStringKeyWithTimestamp("consensus", 3))
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringValue("raft"), value)
}

func TestGetANonExistingKey(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	value, ok := skipList.Get(txn.NewStringKey("consensus"))
	assert.False(t, ok)
	assert.Equal(t, txn.EmptyValue, value)
}

func TestIterateOverSkipList(t *testing.T) {
	skipList := NewSkipList(1 << 10)

	skipList.Put(txn.NewStringKey("consensus"), txn.NewStringValue("raft"))
	skipList.Put(txn.NewStringKey("bolt"), txn.NewStringValue("kv"))
	skipList.Put(txn.NewStringKey("badger"), txn.NewStringValue("LSM"))

	iterator := skipList.NewIterator()
	iterator.SeekToFirst()

	defer func() {
		_ = iterator.Close()
	}()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKey("badger"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("LSM"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKey("bolt"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("kv"), iterator.Value())

	iterator.Next()

	assert.True(t, iterator.Valid())
	assert.Equal(t, txn.NewStringKey("consensus"), iterator.Key())
	assert.Equal(t, txn.NewStringValue("raft"), iterator.Value())

	iterator.Next()
	assert.False(t, iterator.Valid())
}
