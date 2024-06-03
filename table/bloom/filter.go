package bloom

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
	"go-lsm/txn"
	"math"
	"unsafe"
)

const uin8Size = int(unsafe.Sizeof(uint8(0)))

const FalsePositiveRate = 0.01

type Filter struct {
	numberOfHashFunctions uint8
	falsePositiveRate     float64
	bitVector             *bitset.BitSet
}

func DecodeToBloomFilter(buffer []byte, falsePositiveRate float64) (Filter, error) {
	bitVector := new(bitset.BitSet)
	filter := buffer[:len(buffer)-uin8Size]

	if err := bitVector.UnmarshalBinary(filter); err != nil {
		return Filter{}, err
	}
	return Filter{
		numberOfHashFunctions: numberOfHashFunctions(falsePositiveRate),
		falsePositiveRate:     falsePositiveRate,
		bitVector:             bitVector,
	}, nil
}

func (filter Filter) Encode() ([]byte, error) {
	buffer, err := filter.bitVector.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(buffer, filter.numberOfHashFunctions), nil
}

func (filter Filter) MayContain(key txn.Key) bool {
	positions := filter.bitPositionsFor(key)
	for index := 0; index < len(positions); index++ {
		position := positions[index]
		if !filter.bitVector.Test(uint(position)) {
			return false
		}
	}
	return true
}

func (filter Filter) bitPositionsFor(key txn.Key) []uint32 {
	indices := make([]uint32, 0, filter.numberOfHashFunctions)

	for index := uint8(0); index < filter.numberOfHashFunctions; index++ {
		hash := murmur3.Sum32WithSeed(key.Bytes(), uint32(index))
		indices = append(indices, hash%uint32(filter.bitVector.Len()))
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
