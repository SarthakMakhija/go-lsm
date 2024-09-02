package txn

import (
	"context"
	"errors"
	"sync"
)

var ConflictErr = errors.New("transaction conflicts with other concurrent transaction, retry")

// ReadyToCommitTransaction is a concurrently running Readwrite transaction which is ready to be committed.
type ReadyToCommitTransaction struct {
	commitTimestamp uint64
	transaction     *Transaction
}

// Oracle is the central authority that assigns begin and commit timestamp to the transactions.
// Every transaction gets a begin-timestamp and only a Readwrite transaction gets a commit timestamp.
// The current implementation uses next-timestamp which denotes the timestamp that will be assigned as the commit-timestamp
// to the next transaction. The begin-timestamp is one less than the next-timestamp.
// beginTimestampMark is used to indicate till what timestamp have the transactions begun. This information is used to clean up
// the readyToCommitTransactions.
// commitTimestampMark is used to block the new transactions, so all previous commits are visible to a new read.
type Oracle struct {
	lock                      sync.Mutex
	executorLock              sync.Mutex
	nextTimestamp             uint64
	beginTimestampMark        *TransactionTimestampWaterMark
	commitTimestampMark       *TransactionTimestampWaterMark
	executor                  *Executor
	readyToCommitTransactions []ReadyToCommitTransaction
}

// NewOracle creates a new instance of Oracle. It is called once in the entire application.
// Oracle is initialized with nextTimestamp as 1.
// As a part creating a new instance of NewOracle, we also mark beginTimestampMark and commitTimestampMark
// as finished for timestamp 0.
func NewOracle(executor *Executor) *Oracle {
	return NewOracleWithLastCommitTimestamp(executor, 0)
}

// NewOracleWithLastCommitTimestamp creates a new instance of Oracle. It is called once in the entire application.
// Oracle is initialized with nextTimestamp as the lastCommitTimestamp + 1.
// As a part creating a new instance of NewOracle, we also mark beginTimestampMark and commitTimestampMark
// as finished for timestamp lastCommitTimestamp.
func NewOracleWithLastCommitTimestamp(executor *Executor, lastCommitTimestamp uint64) *Oracle {
	oracle := &Oracle{
		nextTimestamp:       lastCommitTimestamp + 1,
		beginTimestampMark:  NewTransactionTimestampWaterMark(),
		commitTimestampMark: NewTransactionTimestampWaterMark(),
		executor:            executor,
	}

	oracle.beginTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.commitTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

// Close stops `beginTimestampMark`, `commitTimestampMark` and `executor`.
func (oracle *Oracle) Close() {
	oracle.beginTimestampMark.Stop()
	oracle.commitTimestampMark.Stop()
	oracle.executor.stop()
}

// beginTimestamp returns the begin-timestamp of a transaction.
// beginTimestamp = nextTimestamp - 1
// Before returning the begin-timestamp, the system performs a wait on the commitTimestampMark.
// This wait is to ensure that all the commits till begin-timestamp are applied in the storage.
func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	oracle.lock.Unlock()

	_ = oracle.commitTimestampMark.WaitForMark(context.Background(), beginTimestamp)
	return beginTimestamp
}

// mayBeCommitTimestampFor returns the commit-timestamp for a  transaction if there are no conflicts.
// A ReadWrite transaction Tx conflicts with other transaction if:
// the keys read by the transaction Tx are modified by another transaction that has the commitTimestamp > beginTimestampOf(Tx).
// If there are no conflicts:
// 1. the current transaction is marked as `beginFinished` by invoking FinishBeginTimestamp.
// 2. readyToCommitTransactions are cleaned up.
// 3. commitTimestamp is assigned to the transaction and the nextTimestamp is increased by 1
// 4. The current transaction is tracked as readyToCommitTransaction
// 5. commitTimestampMark is used to indicate that a transaction with the `commitTimestamp` has begun.
// The cleanupReadyToCommitTransactions removes all the committed transactions Ti...Tj where
// the commitTimestamp of Ti <= maxBeginTransactionTimestamp.
func (oracle *Oracle) mayBeCommitTimestampFor(transaction *Transaction) (uint64, error) {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	if oracle.hasConflictFor(transaction) {
		return 0, ConflictErr
	}

	oracle.FinishBeginTimestamp(transaction)
	oracle.cleanupReadyToCommitTransactions()

	commitTimestamp := oracle.nextTimestamp
	oracle.nextTimestamp = oracle.nextTimestamp + 1

	oracle.trackReadyToCommitTransaction(transaction, commitTimestamp)
	oracle.commitTimestampMark.Begin(commitTimestamp)
	return commitTimestamp, nil
}

// hasConflictFor determines of the transaction has a conflict with other concurrent transactions.
// A Readwrite transaction Tx conflicts with other transaction if:
// the keys read by the transaction Tx are modified by another transaction that has the commitTimestamp > beginTimestampOf(Tx).
// ReadWriteTransaction tracks its read keys in the `reads` property.
func (oracle *Oracle) hasConflictFor(transaction *Transaction) bool {
	for _, committedTransaction := range oracle.readyToCommitTransactions {
		if committedTransaction.commitTimestamp <= transaction.beginTimestamp {
			continue
		}
		for _, key := range transaction.reads {
			if committedTransaction.transaction.batch.Contains(key) {
				return true
			}
		}
	}
	return false
}

// FinishBeginTimestamp indicates that the beginTimestamp of the transaction is finished.
// This is an indication to the TransactionTimestampWaterMark that all the transactions upto a given `beginTimestamp`
// are done. This information will be used in cleaning up the committed transactions.
func (oracle *Oracle) FinishBeginTimestamp(transaction *Transaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

// cleanupReadyToCommitTransactions cleans up the readyToCommitTransactions.
// In order to clean up the transactions the following is done:
// 1. Get the latest beginTimestampMark
// 2. For all the readyToCommitTransactions, if the transaction.commitTimestamp <= maxBeginTransactionTimestamp, skip this transaction
// 3. Create a new array (or slice) of ReadyToCommitTransaction excluding the transactions from step 2.
func (oracle *Oracle) cleanupReadyToCommitTransactions() {
	readyToCommitTransactions := oracle.readyToCommitTransactions[:0]
	maxBeginTransactionTimestamp := oracle.beginTimestampMark.DoneTill()

	for _, transaction := range oracle.readyToCommitTransactions {
		if transaction.commitTimestamp <= maxBeginTransactionTimestamp {
			continue
		}
		readyToCommitTransactions = append(readyToCommitTransactions, transaction)
	}
	oracle.readyToCommitTransactions = readyToCommitTransactions
}

// trackReadyToCommitTransaction tracks all the transactions that are ready to be committed.
func (oracle *Oracle) trackReadyToCommitTransaction(transaction *Transaction, commitTimestamp uint64) {
	oracle.readyToCommitTransactions = append(oracle.readyToCommitTransactions, ReadyToCommitTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
