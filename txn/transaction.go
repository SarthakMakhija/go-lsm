package txn

import (
	"errors"
	"go-lsm"
	"go-lsm/iterator"
	"go-lsm/kv"
)

var EmptyTransactionErr = errors.New("transaction batch is empty, invoke Set in a transaction before committing")

type Transaction struct {
	oracle         *Oracle
	state          *go_lsm.StorageState
	beginTimestamp uint64
	readonly       bool
	batch          *kv.Batch
	reads          []kv.RawKey
}

func NewReadonlyTransaction(oracle *Oracle, state *go_lsm.StorageState) *Transaction {
	return &Transaction{
		oracle:         oracle,
		state:          state,
		beginTimestamp: oracle.beginTimestamp(),
		readonly:       true,
		batch:          nil,
		reads:          nil,
	}
}

func NewReadwriteTransaction(oracle *Oracle, state *go_lsm.StorageState) *Transaction {
	return &Transaction{
		oracle:         oracle,
		state:          state,
		beginTimestamp: oracle.beginTimestamp(),
		readonly:       false,
		batch:          kv.NewBatch(),
		reads:          nil,
	}
}

func (transaction *Transaction) Get(key []byte) (kv.Value, bool) {
	versionedKey := kv.NewKey(key, transaction.beginTimestamp)
	if transaction.readonly {
		return transaction.state.Get(versionedKey)
	}
	transaction.reads = append(transaction.reads, key)
	if value, ok := transaction.batch.Get(key); ok {
		return value, true
	}
	return transaction.state.Get(versionedKey)
}

func (transaction *Transaction) Scan(keyRange kv.InclusiveKeyRange[kv.RawKey]) iterator.Iterator {
	versionedKeyRange := kv.NewInclusiveKeyRange(
		kv.NewKey(keyRange.Start(), transaction.beginTimestamp),
		kv.NewKey(keyRange.End(), transaction.beginTimestamp),
	)
	if transaction.readonly {
		return transaction.state.Scan(versionedKeyRange)
	}
	return nil
}

func (transaction *Transaction) Set(key, value []byte) error {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	return transaction.batch.Put(key, value)
}

func (transaction *Transaction) Commit() (*Future, error) {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	if transaction.batch.IsEmpty() {
		return nil, EmptyTransactionErr
	}

	transaction.oracle.executorLock.Lock()
	defer transaction.oracle.executorLock.Unlock()

	commitTimestamp, err := transaction.oracle.mayBeCommitTimestampFor(transaction)
	if err != nil {
		return nil, err
	}
	commitCallback := func() {
		transaction.oracle.commitTimestampMark.Finish(commitTimestamp)
	}
	return transaction.oracle.executor.submit(kv.NewTimestampedBatchFrom(*transaction.batch, commitTimestamp), commitCallback), nil
}
