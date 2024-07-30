package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKeyIsEqualTo(t *testing.T) {
	key := NewStringKey("consensus")
	assert.True(t, key.IsRawKeyEqualTo(NewStringKey("consensus")))
}

func TestKeyIsNotEqualTo(t *testing.T) {
	key := NewStringKey("consensus")
	assert.False(t, key.IsRawKeyEqualTo(NewStringKey("raft")))
}

func TestKeySize(t *testing.T) {
	key := NewStringKey("consensus")
	assert.Equal(t, 17, key.EncodedSizeInBytes())
}

func TestKeyIsLessThan(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("raft", 10)))
}

func TestKeyIsLessThanBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("consensus", 15)))
}

func TestKeyIsLessThanOrEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyIsLessThanOrEqualToBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.True(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyIsNotLessThanOrEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.False(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("accurate", 10)))
}

func TestKeyIsNotLessThanOrEqualToBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.False(t, key.IsLessThanOrEqualTo(NewStringKeyWithTimestamp("consensus", 5)))
}

func TestKeyComparisonLessThan(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, -1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("distributed", 10)))
}

func TestKeyComparisonLessThanBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 15)
	assert.Equal(t, -1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonEqualTo(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 0, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonEqualToBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 0, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 10)))
}

func TestKeyComparisonGreaterThan(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("accurate", 10)))
}

func TestKeyComparisonGreaterThanBasedOnTimestamp(t *testing.T) {
	key := NewStringKeyWithTimestamp("consensus", 10)
	assert.Equal(t, 1, key.CompareKeysWithDescendingTimestamp(NewStringKeyWithTimestamp("consensus", 20)))
}
