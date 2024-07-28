package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyValueWithAnEmptyString(t *testing.T) {
	value := NewStringValue("")
	assert.True(t, value.IsEmpty())
}

func TestEmptyValueWithNil(t *testing.T) {
	value := NewValue(nil)
	assert.True(t, value.IsEmpty())
}

func TestValueSize(t *testing.T) {
	value := NewStringValue("raft")
	assert.Equal(t, 4, value.SizeInBytes())
}
