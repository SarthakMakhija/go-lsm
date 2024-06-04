package go_lsm

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
