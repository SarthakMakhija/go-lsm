package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	oracle := NewOracle()
	defer oracle.Close()
	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterAPseudoCommit(t *testing.T) {
	oracle := NewOracle()
	defer oracle.Close()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp + 1

	oracle.commitTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(5), oracle.beginTimestamp())
}
