package bloom

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
	"go-lsm/txn"
	"math"
)

type Filter struct {
	capacity              int
	numberOfHashFunctions uint8
	falsePositiveRate     float64
	bitVector             *bitset.BitSet
	bitVectorSize         uint
}

func newBloomFilter(capacity int, falsePositiveRate float64) *Filter {
	if capacity <= 0 {
		panic("capacity must be greater than 0")
	}
	vectorSize := bitVectorSize(capacity, falsePositiveRate)
	return &Filter{
		capacity:              capacity,
		numberOfHashFunctions: numberOfHashFunctions(falsePositiveRate),
		falsePositiveRate:     falsePositiveRate,
		bitVector:             bitset.New(uint(vectorSize)),
		bitVectorSize:         uint(vectorSize),
	}
}

func (bloomFilter *Filter) Put(key txn.Key) {
	positions := bloomFilter.bitPositionsFor(key)
	for index := 0; index < len(positions); index++ {
		position := positions[index]
		bloomFilter.bitVector.Set(uint(position))
	}
}

func (bloomFilter *Filter) Has(key txn.Key) bool {
	positions := bloomFilter.bitPositionsFor(key)
	for index := 0; index < len(positions); index++ {
		position := positions[index]
		if !bloomFilter.bitVector.Test(uint(position)) {
			return false
		}
	}
	return true
}

func (bloomFilter *Filter) bitPositionsFor(key txn.Key) []uint32 {
	indices := make([]uint32, 0, bloomFilter.numberOfHashFunctions)

	for index := uint8(0); index < bloomFilter.numberOfHashFunctions; index++ {
		hash := murmur3.Sum32WithSeed(key.Bytes(), uint32(index))
		indices = append(indices, hash%uint32(bloomFilter.bitVectorSize))
	}
	return indices
}

func numberOfHashFunctions(falsePositiveRate float64) uint8 {
	return uint8(math.Ceil(math.Log2(1.0 / falsePositiveRate)))
}

func bitVectorSize(capacity int, falsePositiveRate float64) int {
	//ln22 = ln2^2
	ln22 := math.Pow(math.Ln2, 2)
	return int(float64(capacity) * math.Abs(math.Log(falsePositiveRate)) / ln22)
}
