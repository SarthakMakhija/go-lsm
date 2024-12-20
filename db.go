package go_lsm

import (
	"errors"
	"fmt"
	"go-lsm/compact"
	"go-lsm/future"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/txn"
	"log/slog"
	"sync/atomic"
	"time"
)

var DbAlreadyStoppedErr = errors.New("db is stopped, can not perform the operation")

// Db represents the key/value database (/storage engine).
type Db struct {
	storageState *state.StorageState
	oracle       *txn.Oracle
	stopped      atomic.Bool
	stopChannel  chan struct{}
}

// KeyValue is an abstraction which contains a key/value pair.
// It is returned from the Scan operation.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// Open opens the database (either new or existing) and creates a new instance of key/value Db.
func Open(options state.StorageOptions) (*Db, error) {
	storageState, err := state.NewStorageStateWithOptions(options)
	if err != nil {
		return nil, err
	}
	db := &Db{
		storageState: storageState,
		oracle:       txn.NewOracleWithLastCommitTimestamp(txn.NewExecutor(storageState), storageState.LastCommitTimestamp()),
		stopChannel:  make(chan struct{}),
	}
	db.startCompaction()
	return db, nil
}

// Read supports read operation by passing an instance of txn.Transaction (via txn.NewReadonlyTransaction) to the callback.
// The passed transaction is a Readonly txn.Transaction which will panic on any form of write and commit operations.
func (db *Db) Read(callback func(transaction *txn.Transaction)) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}
	transaction := txn.NewReadonlyTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	callback(transaction)
	return nil
}

// Write supports writes operation by passing an instance of txn.Transaction via (txn.NewReadwriteTransaction) to the callback.
// The passed transaction is a Readwrite txn.Transaction which supports both read and write operations.
func (db *Db) Write(callback func(transaction *txn.Transaction)) (*future.Future, error) {
	if db.stopped.Load() {
		return nil, DbAlreadyStoppedErr
	}
	transaction := txn.NewReadwriteTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	callback(transaction)
	return transaction.Commit()
}

// Scan supports scan operation by taking an instance of kv.InclusiveKeyRange.
// It returns a slice of KeyValue in increasing order, if no error occurs.
// This implementation only supports kv.InclusiveKeyRange, there is no support for Open and HalfOpen ranges.
func (db *Db) Scan(keyRange kv.InclusiveKeyRange[kv.RawKey]) ([]KeyValue, error) {
	if db.stopped.Load() {
		return nil, DbAlreadyStoppedErr
	}
	transaction := txn.NewReadonlyTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	iterator, err := transaction.Scan(keyRange)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	var keyValuePairs []KeyValue
	for iterator.IsValid() {
		keyValuePairs = append(keyValuePairs, KeyValue{
			Key:   iterator.Key().RawBytes(),
			Value: iterator.Value().Bytes(),
		})
		err := iterator.Next()
		if err != nil {
			return nil, err
		}
	}
	return keyValuePairs, nil
}

// Close closes the database.
// It involves:
// 1. Closing txn.Oracle.
// 2. Closing state.StorageState.
func (db *Db) Close() {
	if db.stopped.CompareAndSwap(false, true) {
		db.oracle.Close()
		db.storageState.Close()
		close(db.stopChannel)
	}
}

// startCompaction start the compaction goroutine.
// It attempts to perform compaction at fixed intervals.
// If compaction happens between 2 levels, it returns a state.StorageStateChangeEvent,
// which is then applied to state.StorageState.
func (db *Db) startCompaction() {
	go func() {
		compactionTimer := time.NewTimer(db.storageState.Options().CompactionOptions.Duration)
		defer compactionTimer.Stop()

		compaction := compact.NewCompaction(db.oracle, db.storageState.SSTableIdGenerator(), db.storageState.Options())
		for {
			select {
			case <-compactionTimer.C:
				storageStateChangeEvent, err := compaction.Start(db.storageState.Snapshot())
				if err != nil {
					slog.Error(fmt.Sprintf("error in starting compaction %v", err))
					return
				}
				if storageStateChangeEvent.HasAnyChanges() {
					if err := db.storageState.Apply(storageStateChangeEvent, false); err != nil {
						slog.Error(fmt.Sprintf("error in apply state change event %v", err))
						return
					}
				}
				compactionTimer.Reset(db.storageState.Options().CompactionOptions.Duration)
			case <-db.stopChannel:
				return
			}
		}
	}()
}
