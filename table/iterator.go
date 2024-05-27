package table

import (
	"encoding/binary"
	"go-lsm/txn"
)

type Block struct {
	data    []byte
	offsets []uint16
}

func NewBlock(data []byte, offsets []uint16) Block {
	return Block{
		data:    data,
		offsets: offsets,
	}
}

type BlockIterator struct {
	key         txn.Key
	value       txn.Value
	offsetIndex uint16
	block       Block
	//the entire value is kept in the iterator. If memory optimization needs to be done,
	// only value range can be key here and the value can be returned from the Value method.
}

func SeekToFirst(block Block) *BlockIterator {
	iterator := &BlockIterator{
		block:       block,
		offsetIndex: 0,
	}
	iterator.seekToOffsetIndex(iterator.offsetIndex)
	return iterator
}

func SeekToKey(block Block, key txn.Key) *BlockIterator {
	iterator := &BlockIterator{
		block: block,
	}
	iterator.seekToGreaterOrEqual(key)
	return iterator
}

func (iterator *BlockIterator) Key() txn.Key {
	return iterator.key
}

func (iterator *BlockIterator) Value() txn.Value {
	return iterator.value
}

func (iterator *BlockIterator) IsValid() bool {
	return !iterator.key.IsEmpty()
}

func (iterator *BlockIterator) Next() error {
	iterator.offsetIndex++
	iterator.seekToOffsetIndex(iterator.offsetIndex)

	return nil
}

func (iterator *BlockIterator) seekToOffsetIndex(index uint16) {
	if index >= uint16(len(iterator.block.offsets)) {
		iterator.markInvalid()
		return
	}
	offset := iterator.block.offsets[index]
	iterator.offsetIndex = index
	iterator.seekToOffset(offset)
}

func (iterator *BlockIterator) seekToGreaterOrEqual(key txn.Key) {
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

func (iterator *BlockIterator) seekToOffset(offset uint16) {
	data := iterator.block.data[offset:]

	keySize := binary.LittleEndian.Uint16(data[:])
	key := txn.NewKey(data[reservedKeySize : uint16(reservedKeySize)+keySize])

	valueSize := binary.LittleEndian.Uint16(data[reservedKeySize+key.Size():])
	valueOffsetStart := uint16(reservedKeySize) + keySize + uint16(reservedValueSize)
	value := txn.NewValue(data[valueOffsetStart : valueOffsetStart+valueSize])

	iterator.key = key
	iterator.value = value
}

func (iterator *BlockIterator) markInvalid() {
	iterator.value = txn.EmptyValue
	iterator.key = txn.EmptyKey
	return
}
