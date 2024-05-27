package table

import (
	"bytes"
	"encoding/binary"
	"go-lsm/txn"
	"unsafe"
)

var reservedKeySize = int(unsafe.Sizeof(uint16(0)))
var reservedValueSize = int(unsafe.Sizeof(uint16(0)))
var uint16Size = int(unsafe.Sizeof(uint16(0)))

type BlockBuilder struct {
	offsets   []uint16
	firstKey  txn.Key
	blockSize uint
	data      *bytes.Buffer
}

// NewBlockBuilder TODO: blockSize should be a multiple of 4096
func NewBlockBuilder(blockSize uint) *BlockBuilder {
	data := new(bytes.Buffer)
	data.Grow(int(blockSize))

	return &BlockBuilder{
		blockSize: blockSize,
		data:      data,
	}
}

func (builder *BlockBuilder) add(key txn.Key, value txn.Value) bool {
	if uint(builder.size()+key.Size()+value.Size()+uint16Size*2 /* key_len, value_len */) > builder.blockSize {
		return false
	}

	if builder.firstKey.IsEmpty() {
		builder.firstKey = key
	}
	builder.offsets = append(builder.offsets, uint16(builder.data.Len()))
	buffer := make([]byte, reservedKeySize+reservedValueSize+key.Size()+value.Size())

	binary.LittleEndian.PutUint16(buffer[:], uint16(key.Size()))
	copy(buffer[reservedKeySize:], key.Bytes())

	binary.LittleEndian.PutUint16(buffer[reservedKeySize+key.Size():], uint16(value.Size()))
	copy(buffer[reservedKeySize+key.Size()+reservedValueSize:], value.Bytes())

	builder.data.Write(buffer)
	return true
}

func (builder *BlockBuilder) isEmpty() bool {
	return len(builder.offsets) == 0
}

func (builder *BlockBuilder) build() Block {
	if builder.isEmpty() {
		panic("cannot build an empty Block")
	}
	return NewBlock(builder.data.Bytes(), builder.offsets)
}

func (builder *BlockBuilder) size() int {
	return builder.data.Len() + len(builder.offsets)*uint16Size
}
