package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"go-lsm/kv"
	"testing"
)

func TestSetsABatchWithOneKeyValueUsingExecutor(t *testing.T) {
	state := go_lsm.NewStorageState()
	defer state.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("kv"), []byte("distributed"))

	executor := NewExecutor(state)
	future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5))
	future.Wait()

	value, ok := state.Get(kv.NewKey([]byte("kv"), 6))
	assert.True(t, ok)
	assert.Equal(t, "distributed", value.String())
}

func TestSetsABatchWithMultipleKeyValuesUsingExecutor(t *testing.T) {
	state := go_lsm.NewStorageState()
	defer state.Close()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("raft"), []byte("consensus"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))

	executor := NewExecutor(state)
	future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5))
	future.Wait()

	value, ok := state.Get(kv.NewKey([]byte("raft"), 6))
	assert.True(t, ok)
	assert.Equal(t, "consensus", value.String())

	value, ok = state.Get(kv.NewKey([]byte("kv"), 6))
	assert.True(t, ok)
	assert.Equal(t, "distributed", value.String())
}

func TestSetsABatchWithMultipleKeyValuesUsingExecutor1(t *testing.T) {
	state := go_lsm.NewStorageState()
	defer state.Close()

	executeSet := func(executor *Executor) {
		batch := kv.NewBatch()
		_ = batch.Put([]byte("raft"), []byte("consensus"))
		future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5))
		future.Wait()
	}

	executeDelete := func(executor *Executor) {
		batch := kv.NewBatch()
		batch.Delete([]byte("raft"))
		future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5))
		future.Wait()
	}

	executor := NewExecutor(state)
	executeSet(executor)
	executeDelete(executor)

	_, ok := state.Get(kv.NewKey([]byte("raft"), 6))
	assert.False(t, ok)
}
