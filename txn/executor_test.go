package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/state"
	"testing"
)

var nothingCallback = func() {}

func TestSetsABatchWithOneKeyValueUsingExecutor(t *testing.T) {
	storageState, _ := state.NewStorageState()
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		storageState.DeleteWALDirectory()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("kv"), []byte("distributed"))

	executor := NewExecutor(storageState)
	future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5), nothingCallback)

	future.Wait()
	assert.True(t, future.Status().IsOk())

	value, ok := storageState.Get(kv.NewKey([]byte("kv"), 6))
	assert.True(t, ok)
	assert.Equal(t, "distributed", value.String())
}

func TestSetsABatchWithOneKeyValueUsingExecutorAndRunsTheCallback(t *testing.T) {
	storageState, _ := state.NewStorageState()
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		storageState.DeleteWALDirectory()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("kv"), []byte("distributed"))

	var applied bool
	executor := NewExecutor(storageState)
	future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5), func() {
		applied = true
	})

	future.Wait()
	assert.True(t, future.Status().IsOk())

	value, ok := storageState.Get(kv.NewKey([]byte("kv"), 6))
	assert.True(t, applied)
	assert.True(t, ok)
	assert.Equal(t, "distributed", value.String())
}

func TestSetsABatchWithMultipleKeyValuesUsingExecutor(t *testing.T) {
	storageState, _ := state.NewStorageState()
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		storageState.DeleteWALDirectory()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("raft"), []byte("consensus"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))

	executor := NewExecutor(storageState)
	future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5), nothingCallback)

	future.Wait()
	assert.True(t, future.Status().IsOk())

	value, ok := storageState.Get(kv.NewKey([]byte("raft"), 6))
	assert.True(t, ok)
	assert.Equal(t, "consensus", value.String())

	value, ok = storageState.Get(kv.NewKey([]byte("kv"), 6))
	assert.True(t, ok)
	assert.Equal(t, "distributed", value.String())
}

func TestSetsABatchWithMultipleKeyValuesUsingExecutor1(t *testing.T) {
	storageState, _ := state.NewStorageState()
	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		storageState.DeleteWALDirectory()
	}()

	executeSet := func(executor *Executor) {
		batch := kv.NewBatch()
		_ = batch.Put([]byte("raft"), []byte("consensus"))
		future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5), nothingCallback)

		future.Wait()
		assert.True(t, future.Status().IsOk())
	}

	executeDelete := func(executor *Executor) {
		batch := kv.NewBatch()
		batch.Delete([]byte("raft"))
		future := executor.submit(kv.NewTimestampedBatchFrom(*batch, 5), nothingCallback)

		future.Wait()
		assert.True(t, future.Status().IsOk())
	}

	executor := NewExecutor(storageState)
	executeSet(executor)
	executeDelete(executor)

	_, ok := storageState.Get(kv.NewKey([]byte("raft"), 6))
	assert.False(t, ok)
}
