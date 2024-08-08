package txn

import (
	"github.com/stretchr/testify/assert"
	"go-lsm"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterAPseudoCommit(t *testing.T) {
	storageState := go_lsm.NewStorageState()
	oracle := NewOracle(NewExecutor(storageState))

	defer func() {
		storageState.Close()
		oracle.Close()
	}()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	oracle.commitTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(5), oracle.beginTimestamp())
}
