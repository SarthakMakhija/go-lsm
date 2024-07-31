package bloom

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestAddAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKeyWithTimestamp("consensus", 10)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.True(t, bloomFilter.MayContain(key))
}

func TestAddAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKeyWithTimestamp("consensus", 20)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.False(t, bloomFilter.MayContain(txn.NewStringKeyWithTimestamp("missing", 20)))
}

func TestEncodeBloomFilter(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKeyWithTimestamp("consensus", 5)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.True(t, decodedBloomFilter.MayContain(txn.NewStringKeyWithTimestamp("consensus", 6)))
}

func TestEncodeBloomFilterContainingAFewKeys(t *testing.T) {
	keys := []txn.Key{
		txn.NewStringKeyWithTimestamp("consensus", 5),
		txn.NewStringKeyWithTimestamp("paxos", 6),
		txn.NewStringKeyWithTimestamp("distributed", 7),
		txn.NewStringKeyWithTimestamp("etcd", 8),
		txn.NewStringKeyWithTimestamp("bolt", 9),
		txn.NewStringKeyWithTimestamp("B+Tree", 10),
		txn.NewStringKeyWithTimestamp("LSM", 11),
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

	queryKeysWithDifferentTimestamps := []txn.Key{
		txn.NewStringKeyWithTimestamp("consensus", 10),
		txn.NewStringKeyWithTimestamp("paxos", 20),
		txn.NewStringKeyWithTimestamp("distributed", 30),
		txn.NewStringKeyWithTimestamp("etcd", 40),
		txn.NewStringKeyWithTimestamp("bolt", 50),
		txn.NewStringKeyWithTimestamp("B+Tree", 60),
		txn.NewStringKeyWithTimestamp("LSM", 70),
	}
	for _, key := range queryKeysWithDifferentTimestamps {
		assert.True(t, decodedBloomFilter.MayContain(key))
	}
}

func TestEncodeBloomFilterAndCheckForNonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := txn.NewStringKeyWithTimestamp("consensus", 5)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.False(t, decodedBloomFilter.MayContain(txn.NewStringKeyWithTimestamp("missing", 5)))
}
