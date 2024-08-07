package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	oracle := NewOracle()
	defer oracle.Stop()
	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterAPseudoCommit(t *testing.T) {
	oracle := NewOracle()
	defer oracle.Stop()

	commitTimestamp := uint64(5)
	oracle.nextTimestamp = commitTimestamp

	oracle.commitTimestampMark.Finish(commitTimestamp)
	assert.Equal(t, uint64(4), oracle.beginTimestamp())
}
