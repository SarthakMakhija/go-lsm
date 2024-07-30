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

const kb uint = 1024
const DefaultBlockSize = 4 * kb

type Builder struct {
	keyValueBeginOffsets []uint16
	firstKey             txn.Key
	blockSize            uint
	data                 []byte
}

// NewBlockBuilder TODO: blockSize should be a multiple of 4096
func NewBlockBuilder(blockSize uint) *Builder {
	return &Builder{
		blockSize: blockSize,
		data:      make([]byte, 0, blockSize),
	}
}

func (builder *Builder) Add(key txn.Key, value txn.Value) bool {
	if uint(builder.size()+key.EncodedSizeInBytes()+value.SizeInBytes()+Uint16Size*2 /* key_len, value_len */) > builder.blockSize {
		return false
	}

	if builder.firstKey.IsRawKeyEmpty() {
		builder.firstKey = key
	}
	builder.keyValueBeginOffsets = append(builder.keyValueBeginOffsets, uint16(len(builder.data)))
	buffer := make([]byte, ReservedKeySize+ReservedValueSize+key.EncodedSizeInBytes()+value.SizeInBytes())

	binary.LittleEndian.PutUint16(buffer[:], uint16(key.EncodedSizeInBytes()))
	copy(buffer[ReservedKeySize:], key.EncodedBytes())

	binary.LittleEndian.PutUint16(buffer[ReservedKeySize+key.EncodedSizeInBytes():], uint16(value.SizeInBytes()))
	copy(buffer[ReservedKeySize+key.EncodedSizeInBytes()+ReservedValueSize:], value.Bytes())

	builder.data = append(builder.data, buffer...)
	return true
}

func (builder *Builder) isEmpty() bool {
	return len(builder.keyValueBeginOffsets) == 0
}

func (builder *Builder) Build() Block {
	if builder.isEmpty() {
		panic("cannot build an empty Block")
	}
	return NewBlock(builder.data, builder.keyValueBeginOffsets)
}

func (builder *Builder) size() int {
	return len(builder.data) + len(builder.keyValueBeginOffsets)*Uint16Size
}
