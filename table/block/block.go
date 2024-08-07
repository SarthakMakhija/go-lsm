package block

import (
	"encoding/binary"
	"go-lsm/kv"
)

type Block struct {
	data                 []byte
	keyValueBeginOffsets []uint16
}

func NewBlock(data []byte, keyValueBeginOffsets []uint16) Block {
	return Block{
		data:                 data,
		keyValueBeginOffsets: keyValueBeginOffsets,
	}
}

func (block Block) Encode() []byte {
	data := block.data
	data = append(data, block.encodeKeyValueBeginOffsets()...)

	numberOfKeyValueBeginOffsets := make([]byte, Uint16Size)
	binary.LittleEndian.PutUint16(numberOfKeyValueBeginOffsets, uint16(len(block.keyValueBeginOffsets)))

	data = append(data, numberOfKeyValueBeginOffsets...)
	return data
}

func DecodeToBlock(data []byte) Block {
	numberOfOffsets := binary.LittleEndian.Uint16(data[len(data)-Uint16Size:])
	startOfOffsets := uint16(len(data)) - uint16(Uint16Size) - numberOfOffsets*uint16(Uint16Size)
	offsetsBuffer := data[startOfOffsets : len(data)-Uint16Size]

	keyValueBeginOffsets := make([]uint16, 0, numberOfOffsets)
	for index := 0; index < len(offsetsBuffer); index += Uint16Size {
		keyValueBeginOffsets = append(keyValueBeginOffsets, binary.LittleEndian.Uint16(offsetsBuffer[index:]))
	}
	return Block{
		data:                 data[:startOfOffsets],
		keyValueBeginOffsets: keyValueBeginOffsets,
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

func (block Block) SeekToKey(key kv.Key) *Iterator {
	iterator := &Iterator{
		block: block,
	}
	iterator.seekToGreaterOrEqual(key)
	return iterator
}

func (block Block) encodeKeyValueBeginOffsets() []byte {
	offsetBuffer := make([]byte, Uint16Size*len(block.keyValueBeginOffsets))
	offsetIndex := 0
	for _, offset := range block.keyValueBeginOffsets {
		binary.LittleEndian.PutUint16(offsetBuffer[offsetIndex:], offset)
		offsetIndex += Uint16Size
	}
	return offsetBuffer
}
