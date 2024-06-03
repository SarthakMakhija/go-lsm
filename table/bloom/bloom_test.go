package bloom

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAddAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	assert.True(t, bloomFilter.Has(key))
}

func TestAddAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	assert.False(t, bloomFilter.Has(txn.NewStringKey("missing")))
}

func TestEncodeBloomFilter(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.True(t, decodedBloomFilter.Has(key))
}

func TestEncodeBloomFilterContainingAFewKeys(t *testing.T) {
	keys := []txn.Key{
		txn.NewStringKey("consensus"),
		txn.NewStringKey("paxos"),
		txn.NewStringKey("distributed"),
		txn.NewStringKey("etcd"),
		txn.NewStringKey("bolt"),
		txn.NewStringKey("B+Tree"),
		txn.NewStringKey("LSM"),
	}

	bloomFilter := newBloomFilter(100, 0.001)
	for _, key := range keys {
		bloomFilter.Put(key)
	}

	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	for _, key := range keys {
		assert.True(t, decodedBloomFilter.Has(key))
	}
}

func TestEncodeBloomFilterAndCheckForNonExistingKey(t *testing.T) {
	bloomFilter := newBloomFilter(20, 0.001)

	key := txn.NewStringKey("consensus")
	bloomFilter.Put(key)

	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.False(t, decodedBloomFilter.Has(txn.NewStringKey("missing")))
}
