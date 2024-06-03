package bloom

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAddAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKey("consensus")
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.True(t, bloomFilter.MayContain(key))
}

func TestAddAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKey("consensus")
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.False(t, bloomFilter.MayContain(txn.NewStringKey("missing")))
}

func TestEncodeBloomFilter(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKey("consensus")
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.True(t, decodedBloomFilter.MayContain(key))
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

	bloomFilterBuilder := NewBloomFilterBuilder()
	for _, key := range keys {
		bloomFilterBuilder.Add(key)
	}

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	for _, key := range keys {
		assert.True(t, decodedBloomFilter.MayContain(key))
	}
}

func TestEncodeBloomFilterAndCheckForNonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKey("consensus")
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.False(t, decodedBloomFilter.MayContain(txn.NewStringKey("missing")))
}
