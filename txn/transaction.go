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

// Transaction represents both Readwrite and Readonly transactions.
// Every transaction gets a begin-timestamp and has an instance of Oracle and state.StorageState.
// An instance of Readwrite transaction maintains:
// - a reference to kv.Batch which is a collection of key/value pairs, that a transaction operates on.
// - a collection of all the keys read within the transaction.
// readLock is used as a lock over the `reads` field, because multiple iterators can be created in a Readwrite transaction.
type Transaction struct {
	oracle         *Oracle
	state          *state.StorageState
	beginTimestamp uint64
	readonly       bool
	batch          *kv.Batch
	reads          []kv.RawKey
	readLock       sync.Mutex
}

// NewReadonlyTransaction creates a new instance of Readonly transaction.
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

// NewReadwriteTransaction creates a new instance of Readwrite transaction.
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

// Get gets the value for the given key.
// It returns a tuple (kv.Value, true), if the key exists, else (kv.EmptyValue, false).
// The Get method involves the following:
// 1) Getting the begin-timestamp of the transaction.
// 2) Getting the value corresponding to the timestamped key from state.StorageState.
// Please note: the system returns the value where the timestamp of the key in the system <= begin-timestamp of the transaction.
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

// Scan supports scan operation by taking an instance of kv.InclusiveKeyRange.
// Scan involves the following:
// 1) Getting the begin-timestamp of the transaction.
// 2) Creating a versionedKeyRange.
// 3) Scanning over state.StorageState if the transaction is a Readonly transaction.
// 4) Scanning over the kv.Batch and state.StorageState if the transaction is a Readwrite transaction.
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

// Set sets the key/value pair in the kv.Batch associated with the Transaction.
// It panics if the same key is added again or the transaction is a Readonly transaction.
func (transaction *Transaction) Set(key, value []byte) error {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	return transaction.batch.Put(key, value)
}

// Delete adds the key in the kv.Batch.
// It panics if the transaction is a Readonly transaction.
func (transaction *Transaction) Delete(key []byte) error {
	if transaction.readonly {
		panic("transaction is readonly")
	}
	transaction.batch.Delete(key)
	return nil
}

// Commit commits the transaction. It panics if the transaction is Readonly or kv.Batch is empty.
// Commit involves the following:
// 1) Acquiring an executorLock to ensure that the transaction are sent to the TransactionExecutor in the order they invoke Commit.
// 2) Getting the commit timestamp for the transaction. Commit timestamp is only provided if the transaction does not have any RW conflict.
// 3) Submitting the kv.TimestampedBatch to the Executor.
// 4) Passing a commit callback to the kv.TimestampedBatch which is invoked when the entire batch is applied.
// 5) The commit callback informs the `commitTimestampMark` of Oracle that a transaction with `commitTimestamp` is done.
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

// trackReads keeps a track of all the keys read in the Readwrite transaction.
func (transaction *Transaction) trackReads(key kv.RawKey) {
	transaction.readLock.Lock()
	transaction.reads = append(transaction.reads, key)
	transaction.readLock.Unlock()
}
