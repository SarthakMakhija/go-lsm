package table

import (
	"encoding/binary"
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

func (block Block) encode() []byte {
	data := block.data
	data = append(data, block.encodeOffsets()...)

	numberOfOffsets := make([]byte, uint16Size)
	binary.LittleEndian.PutUint16(numberOfOffsets, uint16(len(block.offsets)))

	data = append(data, numberOfOffsets...)
	return data
}

func decodeToBlock(data []byte) Block {
	numberOfOffsets := binary.LittleEndian.Uint16(data[len(data)-uint16Size:])
	startOfOffsets := uint16(len(data)) - uint16(uint16Size) - numberOfOffsets*uint16(uint16Size)
	offsetsBuffer := data[startOfOffsets : len(data)-uint16Size]

	offsets := make([]uint16, 0, numberOfOffsets)
	for index := 0; index < len(offsetsBuffer); index += uint16Size {
		offsets = append(offsets, binary.LittleEndian.Uint16(offsetsBuffer[index:]))
	}
	return Block{
		data:    data[:startOfOffsets],
		offsets: offsets,
	}
}

func (block Block) encodeOffsets() []byte {
	offsetBuffer := make([]byte, uint16Size*len(block.offsets))
	offsetIndex := 0
	for _, offset := range block.offsets {
		binary.LittleEndian.PutUint16(offsetBuffer[offsetIndex:], offset)
		offsetIndex += uint16Size
	}
	return offsetBuffer
}
