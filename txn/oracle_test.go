package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/state"
	"go-lsm/test_utility"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterAPseudoCommit(t *testing.T) {
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

	oracle.commitTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(5), oracle.beginTimestamp())
}

func TestGetsCommitTimestampForTransactionGivenNoTransactionsAreCurrentlyTracked(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	transaction := NewReadwriteTransaction(oracle, storageState)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)
}

func TestGetsCommitTimestampForTwoTransactions(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
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
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
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
