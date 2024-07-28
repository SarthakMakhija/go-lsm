package block

import (
	"encoding/binary"
	"go-lsm/txn"
)

type Iterator struct {
	key         txn.Key
	value       txn.Value
	offsetIndex uint16
	block       Block
	//the entire value is kept in the iterator. If memory optimization needs to be done,
	//only value range can be key here and the value can be returned from the Value method.
}

func (iterator *Iterator) Key() txn.Key {
	return iterator.key
}

func (iterator *Iterator) Value() txn.Value {
	return iterator.value
}

func (iterator *Iterator) IsValid() bool {
	return !iterator.key.IsRawKeyEmpty()
}

func (iterator *Iterator) Next() error {
	iterator.offsetIndex++
	iterator.seekToOffsetIndex(iterator.offsetIndex)

	return nil
}

func (iterator *Iterator) Close() {}

func (iterator *Iterator) seekToOffsetIndex(index uint16) {
	if index >= uint16(len(iterator.block.offsets)) {
		iterator.markInvalid()
		return
	}
	offset := iterator.block.offsets[index]
	iterator.offsetIndex = index
	iterator.seekToOffset(offset)
}

func (iterator *Iterator) seekToGreaterOrEqual(key txn.Key) {
	low := 0
	high := len(iterator.block.offsets)

	for low < high {
		mid := low + (high-low)/2
		iterator.seekToOffsetIndex(uint16(mid))

		if !iterator.IsValid() {
			panic("invalid iterator")
		}
		switch iterator.key.Compare(key) {
		case -1:
			low = mid + 1
		case 0:
			return
		case 1:
			high = mid
		}
	}
	iterator.seekToOffsetIndex(uint16(low))
}

func (iterator *Iterator) seekToOffset(offset uint16) {
	data := iterator.block.data[offset:]

	keySize := binary.LittleEndian.Uint16(data[:])
	key := txn.DecodeFrom(data[ReservedKeySize : uint16(ReservedKeySize)+keySize])

	valueSize := binary.LittleEndian.Uint16(data[ReservedKeySize+key.EncodedSizeInBytes():])
	valueOffsetStart := uint16(ReservedKeySize) + keySize + uint16(ReservedValueSize)
	value := txn.NewValue(data[valueOffsetStart : valueOffsetStart+valueSize])

	iterator.key = key
	iterator.value = value
}

func (iterator *Iterator) markInvalid() {
	iterator.value = txn.EmptyValue
	iterator.key = txn.EmptyKey
	return
}
