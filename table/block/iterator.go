package block

import (
	"encoding/binary"
	"go-lsm/kv"
)

// Iterator represents the block iterator.
type Iterator struct {
	key         kv.Key
	value       kv.Value
	offsetIndex uint16
	block       Block
	//the entire value is kept in the iterator. If memory optimization needs to be done,
	//only value range can be key here and the value can be returned from the Value method.
}

// Key returns kv.Key.
func (iterator *Iterator) Key() kv.Key {
	return iterator.key
}

// Value returns kv.Value.
func (iterator *Iterator) Value() kv.Value {
	return iterator.value
}

// IsValid return true if the raw key is not empty.
// Please check iterator.markInvalid().
func (iterator *Iterator) IsValid() bool {
	return !iterator.key.IsRawKeyEmpty()
}

// Next increments the offsetIndex by one and seeks to the incremented offset.
func (iterator *Iterator) Next() error {
	iterator.offsetIndex++
	iterator.seekToOffsetIndex(iterator.offsetIndex)

	return nil
}

// Close does nothing.
func (iterator *Iterator) Close() {}

// seekToOffsetIndex seeks to the offset identify by the index of keyValueBeginOffsets slice.
// If index >= len(iterator.block.keyValueBeginOffsets), iterator is marked invalid.
func (iterator *Iterator) seekToOffsetIndex(index uint16) {
	if index >= uint16(len(iterator.block.keyValueBeginOffsets)) {
		iterator.markInvalid()
		return
	}
	keyValueBeginOffset := iterator.block.keyValueBeginOffsets[index]

	iterator.offsetIndex = index
	iterator.seekToOffset(keyValueBeginOffset)
}

// seekToGreaterOrEqual seeks to the key greater than or equal to the given key.
// It leverages binary search within keyValueBeginOffsets to perform seek.
func (iterator *Iterator) seekToGreaterOrEqual(key kv.Key) {
	low := 0
	high := len(iterator.block.keyValueBeginOffsets) - 1

	for low <= high {
		mid := (low + high) / 2
		iterator.seekToOffsetIndex(uint16(mid))

		if !iterator.IsValid() {
			panic("invalid iterator")
		}
		switch iterator.key.CompareKeysWithDescendingTimestamp(key) {
		case -1:
			low = mid + 1
		case 0:
			return
		case 1:
			high = mid - 1
		}
	}
	iterator.seekToOffsetIndex(uint16(low))
}

// seekToOffset sets the key and value from the offset identified by keyValueBeginOffset.
// Technically, it does not seek to anywhere, it uses the keyValueBeginOffset and decodes
// the key and value.
func (iterator *Iterator) seekToOffset(keyValueBeginOffset uint16) {
	data := iterator.block.data[keyValueBeginOffset:]

	keySize := binary.LittleEndian.Uint16(data[:])
	key := kv.DecodeFrom(data[ReservedKeySize : uint16(ReservedKeySize)+keySize])

	valueSize := binary.LittleEndian.Uint16(data[ReservedKeySize+key.EncodedSizeInBytes():])
	valueOffsetStart := uint16(ReservedKeySize) + keySize + uint16(ReservedValueSize)
	value := kv.NewValue(data[valueOffsetStart : valueOffsetStart+valueSize])

	iterator.key = key
	iterator.value = value
}

// markInvalid marks the iterator invalid by setting the key and value as empty.
func (iterator *Iterator) markInvalid() {
	iterator.value = kv.EmptyValue
	iterator.key = kv.EmptyKey
	return
}
