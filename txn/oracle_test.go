package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/state"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterAPseudoCommit(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	oracle.commitTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(5), oracle.beginTimestamp())
}

func TestGetsCommitTimestampForTransactionGivenNoTransactionsAreCurrentlyTracked(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	transaction := NewReadwriteTransaction(oracle, storageState)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)
}

func TestGetsCommitTimestampForTwoTransactions(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	aTransaction := NewReadwriteTransaction(oracle, storageState)
	aTransaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadwriteTransaction(oracle, storageState)
	anotherTransaction.Get([]byte("SSD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestGetsCommitTimestampForTwoTransactionsGivenOneTransactionReadsTheKeyAfterTheOtherWrites(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	aTransaction := NewReadwriteTransaction(oracle, storageState)
	_ = aTransaction.Set([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.readyToCommitTransactions))

	anotherTransaction := NewReadwriteTransaction(oracle, storageState)
	anotherTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestResultsInConflictErrorForOneTransaction(t *testing.T) {
	storageState, _ := state.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		storageState.DeleteManifest()
		oracle.Close()
	}()

	aTransaction := NewReadwriteTransaction(oracle, storageState)
	_ = aTransaction.Set([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.readyToCommitTransactions))

	anotherTransaction := NewReadwriteTransaction(oracle, storageState)
	_ = anotherTransaction.Set([]byte("HDD"), []byte("Hard disk drive"))
	anotherTransaction.Get([]byte("HDD"))

	thirdTransaction := NewReadwriteTransaction(oracle, storageState)
	thirdTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)

	_, err := oracle.mayBeCommitTimestampFor(thirdTransaction)
	assert.Error(t, err)
	assert.Equal(t, ConflictErr, err)
}
