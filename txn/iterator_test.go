package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/state"
	"testing"
)

func TestIterateOverTransactionIteratorWithAnExistingStateInTheSystem(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("distributed"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.NewStringKeyWithTimestamp("accurate", transaction.beginTimestamp),
		kv.NewStringKeyWithTimestamp("distributed", transaction.beginTimestamp),
	)
	transactionIterator, _ := NewTransactionIterator(transaction, iterator.NewMergeIterator([]iterator.Iterator{
		NewPendingWritesIterator(transaction.batch, transaction.beginTimestamp, kv.NewInclusiveKeyRange(
			kv.RawKey("accurate"),
			kv.RawKey("distributed"),
		)),
		storageState.Scan(keyRange),
	}))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()

	assert.Equal(t, "distributed", transactionIterator.Key().RawString())
	assert.Equal(t, "kv", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithADeletedKeyAndAnExistingStateInTheSystem(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Delete([]byte("distributed"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.NewStringKeyWithTimestamp("accurate", transaction.beginTimestamp),
		kv.NewStringKeyWithTimestamp("distributed", transaction.beginTimestamp),
	)
	transactionIterator, _ := NewTransactionIterator(transaction, iterator.NewMergeIterator([]iterator.Iterator{
		NewPendingWritesIterator(transaction.batch, transaction.beginTimestamp, kv.NewInclusiveKeyRange(
			kv.RawKey("accurate"),
			kv.RawKey("distributed"),
		)),
		storageState.Scan(keyRange),
	}))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithADeletedKeyAndAnExistingDeletedKeyInTheSystem(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	existingBatch.Delete([]byte("consensus"))
	storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Delete([]byte("distributed"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.NewStringKeyWithTimestamp("accurate", transaction.beginTimestamp),
		kv.NewStringKeyWithTimestamp("distributed", transaction.beginTimestamp),
	)
	transactionIterator, _ := NewTransactionIterator(transaction, iterator.NewMergeIterator([]iterator.Iterator{
		NewPendingWritesIterator(transaction.batch, transaction.beginTimestamp, kv.NewInclusiveKeyRange(
			kv.RawKey("accurate"),
			kv.RawKey("distributed"),
		)),
		storageState.Scan(keyRange),
	}))

	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithAnExistingStateInTheSystemWithABoundCheck(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
	oracle.commitTimestampMark.Finish(commitTimestamp)

	transaction := NewReadwriteTransaction(oracle, storageState)
	_ = transaction.Set([]byte("distributed"), []byte("kv"))

	keyRange := kv.NewInclusiveKeyRange(
		kv.NewStringKeyWithTimestamp("accurate", transaction.beginTimestamp),
		kv.NewStringKeyWithTimestamp("consensus", transaction.beginTimestamp),
	)
	transactionIterator, _ := NewTransactionIterator(transaction, iterator.NewMergeIterator([]iterator.Iterator{
		NewPendingWritesIterator(transaction.batch, transaction.beginTimestamp, kv.NewInclusiveKeyRange(
			kv.RawKey("accurate"),
			kv.RawKey("consensus"),
		)),
		storageState.Scan(keyRange),
	}))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}
