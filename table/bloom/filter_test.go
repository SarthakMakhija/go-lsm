package bloom

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"testing"
)

func TestAddAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := kv.NewStringKeyWithTimestamp("consensus", 10)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.True(t, bloomFilter.MayContain(key))
}

func TestAddAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := kv.NewStringKeyWithTimestamp("consensus", 20)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	assert.False(t, bloomFilter.MayContain(kv.NewStringKeyWithTimestamp("missing", 20)))
}

func TestEncodeBloomFilter(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := kv.NewStringKeyWithTimestamp("consensus", 5)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.True(t, decodedBloomFilter.MayContain(kv.NewStringKeyWithTimestamp("consensus", 6)))
}

func TestEncodeBloomFilterContainingAFewKeys(t *testing.T) {
	keys := []kv.Key{
		kv.NewStringKeyWithTimestamp("consensus", 5),
		kv.NewStringKeyWithTimestamp("paxos", 6),
		kv.NewStringKeyWithTimestamp("distributed", 7),
		kv.NewStringKeyWithTimestamp("etcd", 8),
		kv.NewStringKeyWithTimestamp("bolt", 9),
		kv.NewStringKeyWithTimestamp("B+Tree", 10),
		kv.NewStringKeyWithTimestamp("LSM", 11),
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

	queryKeysWithDifferentTimestamps := []kv.Key{
		kv.NewStringKeyWithTimestamp("consensus", 10),
		kv.NewStringKeyWithTimestamp("paxos", 20),
		kv.NewStringKeyWithTimestamp("distributed", 30),
		kv.NewStringKeyWithTimestamp("etcd", 40),
		kv.NewStringKeyWithTimestamp("bolt", 50),
		kv.NewStringKeyWithTimestamp("B+Tree", 60),
		kv.NewStringKeyWithTimestamp("LSM", 70),
	}
	for _, key := range queryKeysWithDifferentTimestamps {
		assert.True(t, decodedBloomFilter.MayContain(key))
	}
}

func TestEncodeBloomFilterAndCheckForNonExistingKey(t *testing.T) {
	bloomFilterBuilder := NewBloomFilterBuilder()

	key := kv.NewStringKeyWithTimestamp("consensus", 5)
	bloomFilterBuilder.Add(key)

	bloomFilter := bloomFilterBuilder.Build(0.001)
	encoded, err := bloomFilter.Encode()
	assert.Nil(t, err)

	decodedBloomFilter, err := DecodeToBloomFilter(encoded, 0.001)
	assert.Nil(t, err)

	assert.False(t, decodedBloomFilter.MayContain(kv.NewStringKeyWithTimestamp("missing", 5)))
}
