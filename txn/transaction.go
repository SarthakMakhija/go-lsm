package txn

import (
	"errors"
	"go-lsm/iterator"
	"go-lsm/kv"
	"go-lsm/state"
	"sync"
)

var EmptyTransactionErr = errors.New("transaction batch is empty, invoke Set in a transaction before committing")

/*
The transaction implementation in the system follows serialized-snapshot-isolation.
A brief background on serialized-snapshot-isolation:
1) Every transaction is given a begin-timestamp. Timestamp is represented as a logical clock.
2) A transaction can read a key with a commit-timestamp < begin-timestamp. This guarantees that the transaction is always reading
   committed data.
3) When a transaction is ready to commit, and there are no conflicts, it is given a commit-timestamp.
4) ReadWrite transactions keep a track of the keys read by them.
   Implementations like [Badger](https://github.com/dgraph-io/badger) keep track of key-hashes inside ReadWrite transactions.
5) Two transactions conflict if there is a read-write conflict. A transaction T2 conflicts with another transaction T1, if,
   T1 has committed to any of the keys read by T2 with a commit-timestamp greater than the begin-timestamp of T2.
6) Readonly transactions never abort.
7) It prevents: dirty-read, fuzzy-read, phantom-read, write-skew and lost-update.
8) Serialized-snapshot-isolation involves keeping a track of `ReadyToCommitTransaction`. Check `Oracle`.

More details are available [here](https://tech-lessons.in/en/blog/serializable_snapshot_isolation/).
*/

type Transaction struct {
	oracle         *Oracle
	state          *state.StorageState
	beginTimestamp uint64
	readonly       bool
	batch          *kv.Batch
	reads          []kv.RawKey
	readLock       sync.Mutex
}

func NewReadonlyTransaction(oracle *Oracle, state *state.StorageState) *Transaction {
	return &Transaction{
		oracle:         oracle,
		state:          state,
		beginTimestamp: oracle.beginTimestamp(),
		readonly:       true,
		batch:          nil,
		reads:          nil,
	}
}

func NewReadwriteTransaction(oracle *Oracle, state *state.StorageState) *Transaction {
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
	transaction.trackReads(key)
	if value, ok := transaction.batch.Get(key); ok {
		return value, true
	}
	return transaction.state.Get(versionedKey)
}

func (transaction *Transaction) Scan(keyRange kv.InclusiveKeyRange[kv.RawKey]) (iterator.Iterator, error) {
	versionedKeyRange := kv.NewInclusiveKeyRange(
		kv.NewKey(keyRange.Start(), transaction.beginTimestamp),
		kv.NewKey(keyRange.End(), transaction.beginTimestamp),
	)
	if transaction.readonly {
		return transaction.state.Scan(versionedKeyRange), nil
	}
	pendingWritesIteratorMergedWithStateIterator := iterator.NewMergeIterator(
		[]iterator.Iterator{
			NewPendingWritesIterator(transaction.batch, transaction.beginTimestamp, keyRange),
			transaction.state.Scan(versionedKeyRange),
		},
	)
	transactionIterator, err := NewTransactionIterator(transaction, pendingWritesIteratorMergedWithStateIterator)
	if err != nil {
		return nil, err
	}
	return transactionIterator, nil
}

func (transaction *Transaction) Set(key, value []byte) error {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	return transaction.batch.Put(key, value)
}

func (transaction *Transaction) Delete(key []byte) error {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	transaction.batch.Delete(key)
	return nil
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

func (transaction *Transaction) trackReads(key kv.RawKey) {
	transaction.readLock.Lock()
	transaction.reads = append(transaction.reads, key)
	transaction.readLock.Unlock()
}
