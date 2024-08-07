package txn

import (
	"context"
	"sync"
)

type Oracle struct {
	lock                sync.Mutex
	executorLock        sync.Mutex
	nextTimestamp       uint64
	beginTimestampMark  *TransactionTimestampWaterMark
	commitTimestampMark *TransactionTimestampWaterMark
}

func NewOracle() *Oracle {
	oracle := &Oracle{
		nextTimestamp:       1,
		beginTimestampMark:  NewTransactionTimestampWaterMark(),
		commitTimestampMark: NewTransactionTimestampWaterMark(),
	}

	oracle.beginTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.commitTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

func (oracle *Oracle) Stop() {
	oracle.beginTimestampMark.Stop()
	oracle.commitTimestampMark.Stop()
}

func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	oracle.lock.Unlock()

	_ = oracle.commitTimestampMark.WaitForMark(context.Background(), beginTimestamp)
	return beginTimestamp
}
