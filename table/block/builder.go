package block

import (
	"encoding/binary"
	"go-lsm/kv"
	"unsafe"
)

var ReservedKeySize = int(unsafe.Sizeof(uint16(0)))
var ReservedValueSize = int(unsafe.Sizeof(uint16(0)))
var Uint16Size = int(unsafe.Sizeof(uint16(0)))
var Uint32Size = int(unsafe.Sizeof(uint32(0)))

const kb uint = 1024
const DefaultBlockSize = 4 * kb

// Builder represents a block builder.
// keyValueBeginOffsets contain the begin-offsets of each of the keys that a part of the block.
// firstKey is the first key of the block.
// data contains the encoded key/value pairs.
//
// Each block contains encoded key/value pairs, and keyValueBeginOffsets. The reason for storing keyValueBeginOffsets is to allow
// binary search for a key within a block. The keyValueBeginOffsets are always in increasing order, hence binary search can be used.
// Please check Block.SeekToKey().
type Builder struct {
	keyValueBeginOffsets []uint16
	firstKey             kv.Key
	blockSize            uint
	data                 []byte
}

// NewBlockBuilder creates a new instance of block builder.
func NewBlockBuilder(blockSize uint) *Builder {
	return &Builder{
		blockSize: blockSize,
		data:      make([]byte, 0, blockSize),
	}
}

// Add adds the key/value pair, along with the begin-offset of the pair in the builder.
// This involves:
// 1) Keeping a track of the first key in the block builder.
// 2) Storing the begin-offset of the key/value pair in keyValueBeginOffsets.
// 3) Storing the key/value pair.
func (builder *Builder) Add(key kv.Key, value kv.Value) bool {
	if uint(builder.size()+key.EncodedSizeInBytes()+value.SizeInBytes()+ReservedKeySize+ReservedValueSize) > builder.blockSize {
		return false
	}

	if builder.firstKey.IsRawKeyEmpty() {
		builder.firstKey = key
	}
	builder.keyValueBeginOffsets = append(builder.keyValueBeginOffsets, uint16(len(builder.data)))
	keyValueBuffer := make([]byte, ReservedKeySize+ReservedValueSize+key.EncodedSizeInBytes()+value.SizeInBytes())

	binary.LittleEndian.PutUint16(keyValueBuffer[:], uint16(key.EncodedSizeInBytes()))
	copy(keyValueBuffer[ReservedKeySize:], key.EncodedBytes())

	binary.LittleEndian.PutUint16(keyValueBuffer[ReservedKeySize+key.EncodedSizeInBytes():], uint16(value.SizeInBytes()))
	copy(keyValueBuffer[ReservedKeySize+key.EncodedSizeInBytes()+ReservedValueSize:], value.Bytes())

	builder.data = append(builder.data, keyValueBuffer...)
	return true
}

// isEmpty returns true if the builder has not stored any key/value pair.
func (builder *Builder) isEmpty() bool {
	return len(builder.keyValueBeginOffsets) == 0
}

// Build creates a new instance of Block.
func (builder *Builder) Build() Block {
	if builder.isEmpty() {
		panic("cannot build an empty Block")
	}
	return newBlock(builder.data, builder.keyValueBeginOffsets)
}

// size returns the size of the builder.
// The size includes: the size of encoded key/values (builder.data) + size of N keyValueBeginOffsets.
func (builder *Builder) size() int {
	return len(builder.data) +
		len(builder.keyValueBeginOffsets)*Uint16Size +
		Uint16Size //block uses last 2 bytes for the number of begin offsets
}
