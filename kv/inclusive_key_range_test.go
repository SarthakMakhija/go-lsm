package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalidInclusiveRangeGivenEndKeyIsSmallerThanTheStartKey(t *testing.T) {
	assert.Panics(t, func() {
		NewInclusiveKeyRange(NewStringKeyWithTimestamp("consensus", 10), NewStringKeyWithTimestamp("accurate", 10))
	})
}

func TestInvalidInclusiveRangeGivenEndKeyIsSmallerThanTheStartKeyBasedOnTimestamp(t *testing.T) {
	assert.Panics(t, func() {
		NewInclusiveKeyRange(NewStringKeyWithTimestamp("consensus", 10), NewStringKeyWithTimestamp("consensus", 5))
	})
}

func TestInclusiveRange(t *testing.T) {
	inclusiveRange := NewInclusiveKeyRange(NewStringKeyWithTimestamp("consensus", 10), NewStringKeyWithTimestamp("distributed", 5))
	assert.Equal(t, NewStringKeyWithTimestamp("consensus", 10), inclusiveRange.Start())
	assert.Equal(t, NewStringKeyWithTimestamp("distributed", 5), inclusiveRange.End())
}
