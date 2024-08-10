package go_lsm

import (
	"errors"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/txn"
	"sync/atomic"
)

var DbAlreadyStoppedErr = errors.New("db is stopped, can not perform the operation")

type Db struct {
	storageState *state.StorageState
	oracle       *txn.Oracle
	stopped      atomic.Bool
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

func NewDb(options state.StorageOptions) *Db {
	storageState := state.NewStorageStateWithOptions(options)
	return &Db{
		storageState: storageState,
		oracle:       txn.NewOracle(txn.NewExecutor(storageState)),
	}
}

func (db *Db) Read(callback func(transaction *txn.Transaction)) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}
	transaction := txn.NewReadonlyTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	callback(transaction)
	return nil
}

func (db *Db) Write(callback func(transaction *txn.Transaction)) (*txn.Future, error) {
	if db.stopped.Load() {
		return nil, DbAlreadyStoppedErr
	}
	transaction := txn.NewReadwriteTransaction(db.oracle, db.storageState)
	defer db.oracle.FinishBeginTimestamp(transaction)

	callback(transaction)
	return transaction.Commit()
}

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

func (db *Db) Close() {
	if db.stopped.CompareAndSwap(false, true) {
		db.oracle.Close()
		db.storageState.Close()
	}
}
