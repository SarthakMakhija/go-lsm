package bloom

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
	"go-lsm/kv"
	"math"
	"unsafe"
)

const uin8Size = int(unsafe.Sizeof(uint8(0)))

const FalsePositiveRate = 0.01

// Filter represents Bloom filter.
// Bloom filter is a probabilistic data structure used to test whether an element is a add member.
// A bloom filter can query against large amounts of data and return either “possibly in the add” or “definitely not in the add”.
// It depends on M-sized bit vector and K-hash functions.
type Filter struct {
	numberOfHashFunctions uint8
	falsePositiveRate     float64
	bitVector             *bitset.BitSet
}

// DecodeToBloomFilter decodes the byte slice to the bloom filter.
// It relies on bitset.BitSet for decoding.
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

// Encode encodes the filter to a byte slice.
// It relies on bitset.BitSet for encoding. The encoded format is:
/*
  ------------------------------------------------
 | bit vector | 1 byte for numberOfHashFunctions  |
  ------------------------------------------------
*/
func (filter Filter) Encode() ([]byte, error) {
	buffer, err := filter.bitVector.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(buffer, filter.numberOfHashFunctions), nil
}

// add adds the given key in the bloom filter by setting the positions (/indices) of the key in the bit vector.
func (filter Filter) add(key kv.Key) {
	positions := filter.bitPositionsFor(key)
	for index := 0; index < len(positions); index++ {
		position := positions[index]
		filter.bitVector.Set(uint(position))
	}
}

// MayContain returns true if all the bits identified by the positions (/indices) for the key are add.
// True indicates that the key MAYBE present in the system.
// Returns false, if any of the bits identified by the positions (/indices) for the key are not add.
// False indicates that the key is definitely NOT present in the system.
func (filter Filter) MayContain(key kv.Key) bool {
	positions := filter.bitPositionsFor(key)
	for index := 0; index < len(positions); index++ {
		position := positions[index]
		if !filter.bitVector.Test(uint(position)) {
			return false
		}
	}
	return true
}

// bitPositionsFor returns the bit vector positions (/indices) for the key which must either be added or checked.
func (filter Filter) bitPositionsFor(key kv.Key) []uint32 {
	indices := make([]uint32, 0, filter.numberOfHashFunctions)

	for index := uint8(0); index < filter.numberOfHashFunctions; index++ {
		hash := murmur3.Sum32WithSeed(key.RawBytes(), uint32(index))
		indices = append(indices, hash%uint32(filter.bitVector.Len()))
	}
	return indices
}

// numberOfHashFunctions returns the number of hash functions.
func numberOfHashFunctions(falsePositiveRate float64) uint8 {
	return uint8(math.Ceil(math.Log2(1.0 / falsePositiveRate)))
}

// bitVectorSize returns the bit vector size.
func bitVectorSize(capacity int, falsePositiveRate float64) int {
	//ln22 = ln2^2
	ln22 := math.Pow(math.Ln2, 2)
	return int(float64(capacity) * math.Abs(math.Log(falsePositiveRate)) / ln22)
}
