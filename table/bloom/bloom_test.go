package bloom

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	assert.True(t, bloomFilter.Has(key))
}

func TestAddsAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	assert.False(t, bloomFilter.Has(txn.NewStringKey("missing")))
}
