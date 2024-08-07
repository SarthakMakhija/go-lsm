package txn

import (
	go_lsm "go-lsm"
	"go-lsm/iterator"
	"go-lsm/kv"
)

type Transaction struct {
	oracle         *Oracle
	state          *go_lsm.StorageState
	beginTimestamp uint64
	readonly       bool
}

func NewReadonlyTransaction(oracle *Oracle, state *go_lsm.StorageState) *Transaction {
	return &Transaction{
		oracle:         oracle,
		state:          state,
		beginTimestamp: oracle.beginTimestamp(),
		readonly:       true,
	}
}

func (transaction *Transaction) Get(key []byte) (kv.Value, bool) {
	versionedKey := kv.NewKey(key, transaction.beginTimestamp)
	if transaction.readonly {
		return transaction.state.Get(versionedKey)
	}
	return kv.EmptyValue, false
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
