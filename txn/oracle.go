package txn

import (
	"context"
	"errors"
	"sync"
)

var ConflictErr = errors.New("transaction conflicts with other concurrent transaction, retry")

type ReadyToCommitTransaction struct {
	commitTimestamp uint64
	transaction     *Transaction
}

type Oracle struct {
	lock                      sync.Mutex
	executorLock              sync.Mutex
	nextTimestamp             uint64
	beginTimestampMark        *TransactionTimestampWaterMark
	commitTimestampMark       *TransactionTimestampWaterMark
	executor                  *Executor
	readyToCommitTransactions []ReadyToCommitTransaction
}

func NewOracle(executor *Executor) *Oracle {
	oracle := &Oracle{
		nextTimestamp:       1,
		beginTimestampMark:  NewTransactionTimestampWaterMark(),
		commitTimestampMark: NewTransactionTimestampWaterMark(),
		executor:            executor,
	}

	oracle.beginTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.commitTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

func (oracle *Oracle) Close() {
	oracle.beginTimestampMark.Stop()
	oracle.commitTimestampMark.Stop()
	oracle.executor.stop()
}

func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	oracle.lock.Unlock()

	_ = oracle.commitTimestampMark.WaitForMark(context.Background(), beginTimestamp)
	return beginTimestamp
}

func (oracle *Oracle) mayBeCommitTimestampFor(transaction *Transaction) (uint64, error) {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	if oracle.hasConflictFor(transaction) {
		return 0, ConflictErr
	}

	oracle.finishBeginTimestamp(transaction)
	oracle.cleanupCommittedTransactions()

	commitTimestamp := oracle.nextTimestamp
	oracle.nextTimestamp = oracle.nextTimestamp + 1

	oracle.trackReadyToCommitTransaction(transaction, commitTimestamp)
	oracle.commitTimestampMark.Begin(commitTimestamp)
	return commitTimestamp, nil
}

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

// TODO: also call this when a readonly transaction finishes
func (oracle *Oracle) finishBeginTimestamp(transaction *Transaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) cleanupCommittedTransactions() {
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

func (oracle *Oracle) trackReadyToCommitTransaction(transaction *Transaction, commitTimestamp uint64) {
	oracle.readyToCommitTransactions = append(oracle.readyToCommitTransactions, ReadyToCommitTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
