package txn

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestTransactionTimestampMarkWithASingleTransaction(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(1), transactionTimestampMark.DoneTill())
}

func TestTransactionTimestampMarkWithTwoTransactions(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Begin(2)

	transactionTimestampMark.Finish(2)
	transactionTimestampMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionTimestampMark.DoneTill())
}

func TestTransactionTimestampMarkWithAFewTransactions(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Begin(2)

	transactionTimestampMark.Finish(2)
	transactionTimestampMark.Finish(1)
	transactionTimestampMark.Finish(1)
	transactionTimestampMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionTimestampMark.DoneTill())
}

func TestTransactionTimestampMarkWithTwoConcurrentTransactions(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		transactionTimestampMark.Begin(1)
		transactionTimestampMark.Finish(1)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		transactionTimestampMark.Begin(2)
		transactionTimestampMark.Finish(2)
	}()

	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionTimestampMark.DoneTill())
}

func TestTransactionTimestampMarkWithConcurrentTransactions(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()

	var wg sync.WaitGroup
	wg.Add(100)

	for count := 1; count <= 100; count++ {
		go func(index uint64) {
			defer wg.Done()
			transactionTimestampMark.Begin(index)
			transactionTimestampMark.Finish(index)
		}(uint64(count))
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, uint64(100), transactionTimestampMark.DoneTill())
}

func TestTransactionMarkAndWaitForATimestamp(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	go func() {
		transactionTimestampMark.Begin(1)
		time.Sleep(10 * time.Millisecond)
		transactionTimestampMark.Finish(1)
	}()

	err := transactionTimestampMark.WaitForMark(context.Background(), 1)

	assert.Nil(t, err)
}

func TestTransactionMarkAndWaitForAnAlreadyFinishedTimestamp(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Finish(1)

	err := transactionTimestampMark.WaitForMark(context.Background(), 1)
	assert.Nil(t, err)
}

func TestTransactionMarkAndTimeoutWaitingForAnUnfinishedTimestamp(t *testing.T) {
	transactionTimestampMark := NewTransactionTimestampMark()
	transactionTimestampMark.Begin(1)
	transactionTimestampMark.Finish(1)

	ctx, cancelFunction := context.WithTimeout(context.Background(), 15*time.Millisecond)
	err := transactionTimestampMark.WaitForMark(ctx, 2)

	assert.Error(t, err)
	cancelFunction()
}
