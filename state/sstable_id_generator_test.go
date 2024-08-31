package state

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateFirstMemtableId(t *testing.T) {
	generator := NewSSTableIdGenerator()
	assert.Equal(t, uint64(1), generator.NextId())
}

func TestGenerateNextMemtableId(t *testing.T) {
	generator := NewSSTableIdGenerator()
	assert.Equal(t, uint64(1), generator.NextId())
	assert.Equal(t, uint64(2), generator.NextId())
	assert.Equal(t, uint64(3), generator.NextId())
}

func TestSetIdIfGreaterThanExisting(t *testing.T) {
	generator := NewSSTableIdGenerator()
	generator.NextId()

	generator.setIdIfGreaterThanExisting(10)
	assert.Equal(t, uint64(11), generator.NextId())
}

func TestDoesNotSetIdGivenItIsNotGreaterThanExisting(t *testing.T) {
	generator := NewSSTableIdGenerator()
	generator.NextId()
	generator.NextId()

	generator.setIdIfGreaterThanExisting(1)
	assert.Equal(t, uint64(3), generator.NextId())
}
