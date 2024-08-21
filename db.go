package go_lsm

import (
	"errors"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/txn"
	"sync/atomic"
)

var DbAlreadyStoppedErr = errors.New("db is stopped, can not perform the operation")

// Db represents the key/value database (/storage engine).
type Db struct {
	storageState *state.StorageState
	oracle       *txn.Oracle
	stopped      atomic.Bool
}

// KeyValue is an abstraction which contains a key/value pair.
// It is returned from the Scan operation.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// NewDb creates a new instance of Db.
func NewDb(options state.StorageOptions) *Db {
	storageState := state.NewStorageStateWithOptions(options)
	return &Db{
		storageState: storageState,
		oracle:       txn.NewOracle(txn.NewExecutor(storageState)),
	}
}

// Read supports read operation by passing an instance of txn.NewReadonlyTransaction to the callback.
func (db *Db) Read(callback func(transaction *txn.Transaction)) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}
	transaction := txn.NewReadonlyTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	callback(transaction)
	return nil
}

// Read supports read operation by passing an instance of txn.NewReadwriteTransaction to the callback.
func (db *Db) Write(callback func(transaction *txn.Transaction)) (*txn.Future, error) {
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
	}
}
