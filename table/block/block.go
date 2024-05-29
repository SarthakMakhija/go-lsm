package block

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

func (block Block) Encode() []byte {
	data := block.data
	data = append(data, block.encodeOffsets()...)
	numberOfOffsets := make([]byte, Uint16Size)
	binary.LittleEndian.PutUint16(numberOfOffsets, uint16(len(block.offsets)))

	data = append(data, numberOfOffsets...)
	return data
}

func DecodeToBlock(data []byte) Block {
	numberOfOffsets := binary.LittleEndian.Uint16(data[len(data)-Uint16Size:])
	startOfOffsets := uint16(len(data)) - uint16(Uint16Size) - numberOfOffsets*uint16(Uint16Size)
	offsetsBuffer := data[startOfOffsets : len(data)-Uint16Size]

	offsets := make([]uint16, 0, numberOfOffsets)
	for index := 0; index < len(offsetsBuffer); index += Uint16Size {
		offsets = append(offsets, binary.LittleEndian.Uint16(offsetsBuffer[index:]))
	}
	return Block{
		data:    data[:startOfOffsets],
		offsets: offsets,
	}
}

func (block Block) SeekToFirst() *Iterator {
	iterator := &Iterator{
		block:       block,
		offsetIndex: 0,
	}
	iterator.seekToOffsetIndex(iterator.offsetIndex)
	return iterator
}

func (block Block) SeekToKey(key txn.Key) *Iterator {
	iterator := &Iterator{
		block: block,
	}
	iterator.seekToGreaterOrEqual(key)
	return iterator
}

func (block Block) encodeOffsets() []byte {
	offsetBuffer := make([]byte, Uint16Size*len(block.offsets))
	offsetIndex := 0
	for _, offset := range block.offsets {
		binary.LittleEndian.PutUint16(offsetBuffer[offsetIndex:], offset)
		offsetIndex += Uint16Size
	}
	return offsetBuffer
}
