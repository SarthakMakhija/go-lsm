package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"go-lsm/kv"
	"testing"
)

func TestReadonlyTransactionWithEmptyState(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

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
	oracle := NewOracle(NewExecutor(storageState))

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
	oracle := NewOracle(NewExecutor(storageState))

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
	oracle := NewOracle(NewExecutor(storageState))

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
	iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("draft"), kv.RawKey("quadrant")))

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestReadonlyTransactionWithScanHavingSameKeyWithMultipleTimestamps(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

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
	iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("bolt"), kv.RawKey("quadrant")))

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "VSR", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestAttemptsToCommitAnEmptyReadwriteTransaction(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	oracle.commitTimestampMark.Finish(2)
	transaction := NewReadwriteTransaction(oracle, storageState)

	_, err := transaction.Commit()

	assert.Error(t, err)
	assert.Equal(t, EmptyTransactionErr, err)
}

func TestGetsAnExistingKeyInAReadwriteTransaction(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("HDD"), []byte("Hard disk"))
	future, _ := transaction.Commit()
	future.Wait()

	anotherTransaction := NewReadwriteTransaction(oracle, storageState)
	_ = anotherTransaction.Set([]byte("SSD"), []byte("Solid state drive"))
	future, _ = anotherTransaction.Commit()
	future.Wait()

	readonlyTransaction := NewReadonlyTransaction(oracle, storageState)

	value, ok := readonlyTransaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk", value.String())

	value, ok = readonlyTransaction.Get([]byte("SSD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Solid state drive", value.String())

	_, ok = readonlyTransaction.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestGetsTheValueFromAKeyInAReadwriteTransactionFromBatch(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("HDD"), []byte("Hard disk"))

	value, ok := transaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk", value.String())

	future, _ := transaction.Commit()
	future.Wait()
}

func TestTracksReadsInAReadwriteTransactionWithGet(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("SSD"))

	future, _ := transaction.Commit()
	future.Wait()

	assert.Equal(t, 1, len(transaction.reads))
	assert.Equal(t, kv.RawKey("SSD"), transaction.reads[0])
}

func TestReadwriteTransactionWithScanHavingMultipleTimestampsOfSameKey(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

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

	transaction := NewReadwriteTransaction(oracle, storageState)
	iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("bolt"), kv.RawKey("quadrant")))

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "VSR", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.False(t, iterator.IsValid())
}

func TestReadwriteTransactionWithScanHavingDeletedKey(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	batch := kv.NewBatch()
	batch.Delete([]byte("quadrant"))
	_ = batch.Put([]byte("consensus"), []byte("VSR"))
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("bolt"), kv.RawKey("rocks")))

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "VSR", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())
}

func TestTracksReadsInAReadwriteTransactionWithScan(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	batch := kv.NewBatch()
	batch.Delete([]byte("quadrant"))
	_ = batch.Put([]byte("consensus"), []byte("VSR"))
	_ = batch.Put([]byte("storage"), []byte("NVMe"))
	_ = batch.Put([]byte("kv"), []byte("distributed"))
	storageState.Set(kv.NewTimestampedBatchFrom(*batch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("hdd"), []byte("Hard disk"))

	iterator, _ := transaction.Scan(kv.NewInclusiveKeyRange(kv.RawKey("bolt"), kv.RawKey("tiger-beetle")))

	assert.Equal(t, "consensus", iterator.Key().RawString())
	assert.Equal(t, "VSR", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "hdd", iterator.Key().RawString())
	assert.Equal(t, "Hard disk", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "kv", iterator.Key().RawString())
	assert.Equal(t, "distributed", iterator.Value().String())

	_ = iterator.Next()

	assert.Equal(t, "storage", iterator.Key().RawString())
	assert.Equal(t, "NVMe", iterator.Value().String())

	_ = iterator.Next()
	assert.False(t, iterator.IsValid())

	allTrackedReads := transaction.reads
	assert.Equal(t, 4, len(allTrackedReads))

	assert.Equal(t, "consensus", string(allTrackedReads[0]))
	assert.Equal(t, "hdd", string(allTrackedReads[1]))
	assert.Equal(t, "kv", string(allTrackedReads[2]))
	assert.Equal(t, "storage", string(allTrackedReads[3]))
}
