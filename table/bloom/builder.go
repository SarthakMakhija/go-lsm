package bloom

import (
	"github.com/bits-and-blooms/bitset"
	"go-lsm/txn"
)

type FilterBuilder struct {
	keys []txn.Key
}

func NewBloomFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

func (builder *FilterBuilder) Add(key txn.Key) {
	builder.keys = append(builder.keys, key)
}

func (builder *FilterBuilder) Build(falsePositiveRate float64) Filter {
	vectorSize := bitVectorSize(len(builder.keys), falsePositiveRate)
	filter := Filter{
		numberOfHashFunctions: numberOfHashFunctions(falsePositiveRate),
		falsePositiveRate:     falsePositiveRate,
		bitVector:             bitset.New(uint(vectorSize)),
	}
	for _, key := range builder.keys {
		positions := filter.bitPositionsFor(key)
		for index := 0; index < len(positions); index++ {
			position := positions[index]
			filter.bitVector.Set(uint(position))
		}
	}
	return filter
}
