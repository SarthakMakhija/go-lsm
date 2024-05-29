package block

import (
	"encoding/binary"
	"go-lsm/txn"
	"unsafe"
)

var ReservedKeySize = int(unsafe.Sizeof(uint16(0)))
var ReservedValueSize = int(unsafe.Sizeof(uint16(0)))
var Uint16Size = int(unsafe.Sizeof(uint16(0)))
var Uint32Size = int(unsafe.Sizeof(uint32(0)))

type Builder struct {
	offsets          []uint16
	firstKey         txn.Key
	blockSize        uint
	data             []byte
	currentDataIndex int
}

// NewBlockBuilder TODO: blockSize should be a multiple of 4096
func NewBlockBuilder(blockSize uint) *Builder {
	return &Builder{
		blockSize: blockSize,
		data:      make([]byte, 0, blockSize),
	}
}

func (builder *Builder) Add(key txn.Key, value txn.Value) bool {
	if uint(builder.size()+key.Size()+value.Size()+Uint16Size*2 /* key_len, value_len */) > builder.blockSize {
		return false
	}

	if builder.firstKey.IsEmpty() {
		builder.firstKey = key
	}
	builder.offsets = append(builder.offsets, uint16(len(builder.data)))
	buffer := make([]byte, ReservedKeySize+ReservedValueSize+key.Size()+value.Size())

	binary.LittleEndian.PutUint16(buffer[:], uint16(key.Size()))
	copy(buffer[ReservedKeySize:], key.Bytes())

	binary.LittleEndian.PutUint16(buffer[ReservedKeySize+key.Size():], uint16(value.Size()))
	copy(buffer[ReservedKeySize+key.Size()+ReservedValueSize:], value.Bytes())

	builder.data = append(builder.data, buffer...)
	builder.currentDataIndex += len(buffer)

	return true
}

func (builder *Builder) isEmpty() bool {
	return len(builder.offsets) == 0
}

func (builder *Builder) Build() Block {
	if builder.isEmpty() {
		panic("cannot build an empty Block")
	}
	return NewBlock(builder.data, builder.offsets)
}

func (builder *Builder) size() int {
	return len(builder.data) + len(builder.offsets)*Uint16Size
}
