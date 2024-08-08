package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"go-lsm/kv"
	"testing"
)

func TestReadonlyTransactionWithEmptyState(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle()

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	transaction := NewReadonlyTransaction(oracle, storageState)
	_, ok := transaction.Get([]byte("paxos"))

	assert.False(t, ok)
}

func TestReadonlyTransactionWithAnExistingKey(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle()

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadonlyTransaction(oracle, storageState)
	value, ok := transaction.Get([]byte("consensus"))

	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())
}

func TestReadonlyTransactionWithAnExistingKeyButWithATimestampHigherThanCommitTimestamp(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle()

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	//simulate a readonly transaction starting first
	oracle.nextTimestamp = uint64(5)
	oracle.commitTimestampMark.Finish(uint64(4))
	transaction := NewReadonlyTransaction(oracle, storageState)

	commitTimestamp := uint64(6)
	batch := kv.NewBatch()
	_ = batch.Put([]byte("raft"), []byte("consensus algorithm"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	_, ok := transaction.Get([]byte("raft"))

	assert.False(t, ok)
}

func TestReadonlyTransactionWithScan(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle()

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("raft"))
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadonlyTransaction(oracle, storageState)
	iterator := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("draft"), kv.RawKey("quadrant")))

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestReadonlyTransactionWithScanHavingSameKeyWithMultipleTimestamps(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle()

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	batch := kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("unknown"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, 4))

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	batch = kv.NewBatch()
	_ = batch.Put([]byte("consensus"), []byte("VSR"))
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadonlyTransaction(oracle, storageState)
	iterator := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("bolt"), kv.RawKey("quadrant")))

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "VSR", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}
