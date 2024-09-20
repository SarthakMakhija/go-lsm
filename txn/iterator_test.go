package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/test_utility"
	"testing"
)

func TestIterateOverTransactionIteratorWithAnExistingStateInTheSystem(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	_ = storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
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
	}, iterator.NoOperationOnCloseCallback))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()

	assert.Equal(t, "distributed", transactionIterator.Key().RawString())
	assert.Equal(t, "kv", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithADeletedKeyAndAnExistingStateInTheSystem(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	_ = storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
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
	}, iterator.NoOperationOnCloseCallback))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithADeletedKeyAndAnExistingDeletedKeyInTheSystem(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	existingBatch.Delete([]byte("consensus"))
	_ = storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
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
	}, iterator.NoOperationOnCloseCallback))

	assert.False(t, transactionIterator.IsValid())
}

func TestIterateOverTransactionIteratorWithAnExistingStateInTheSystemWithABoundCheck(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	existingBatch := kv.NewBatch()
	_ = existingBatch.Put([]byte("consensus"), []byte("raft"))
	_ = storageState.Set(kv.NewTimestampedBatchFrom(*existingBatch, commitTimestamp))
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
	}, iterator.NoOperationOnCloseCallback))

	assert.Equal(t, "consensus", transactionIterator.Key().RawString())
	assert.Equal(t, "raft", transactionIterator.Value().String())

	_ = transactionIterator.Next()
	assert.False(t, transactionIterator.IsValid())
}
