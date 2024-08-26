package bloom

import (
	"github.com/bits-and-blooms/bitset"
	"go-lsm/kv"
)

// FilterBuilder represents bloom filter builder.
type FilterBuilder struct {
	keys []kv.Key
}

// NewBloomFilterBuilder creates a new instance of bloom filter builder.
func NewBloomFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

// Add adds the given key to its collection.
func (builder *FilterBuilder) Add(key kv.Key) {
	builder.keys = append(builder.keys, key)
}

// Build builds a new bloom filter.
// It involves the following:
// 1) Determining the bit vector size.
// 2) Creating a new instance of bloom filter.
// 3) Adding all the keys in the bloom filter.
func (builder *FilterBuilder) Build(falsePositiveRate float64) Filter {
	vectorSize := bitVectorSize(len(builder.keys), falsePositiveRate)
	filter := Filter{
		numberOfHashFunctions: numberOfHashFunctions(falsePositiveRate),
		falsePositiveRate:     falsePositiveRate,
		bitVector:             bitset.New(uint(vectorSize)),
	}
	for _, key := range builder.keys {
		filter.add(key)
	}
	return filter
}
