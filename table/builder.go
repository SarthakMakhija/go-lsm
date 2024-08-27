package table

import (
	"bytes"
	"encoding/binary"
	"go-lsm/kv"
	"go-lsm/table/block"
	"go-lsm/table/bloom"
)

// SSTableBuilder allows building SSTable in a step-by-step manner.
type SSTableBuilder struct {
	blockBuilder       *block.Builder
	blockMetaList      *block.MetaList
	bloomFilterBuilder *bloom.FilterBuilder
	startingKey        kv.Key
	endingKey          kv.Key
	allBlocksData      []byte
	blockSize          uint
}

// NewSSTableBuilderWithDefaultBlockSize creates a new instance of SSTableBuilder with block.DefaultBlockSize = 4Kb.
func NewSSTableBuilderWithDefaultBlockSize() *SSTableBuilder {
	return NewSSTableBuilder(block.DefaultBlockSize)
}

// NewSSTableBuilder creates a new instance of SSTableBuilder with the given block size.
// The specified block size will be used to limit the size of each block that will be a part of the final SSTable.
func NewSSTableBuilder(blockSize uint) *SSTableBuilder {
	return &SSTableBuilder{
		blockBuilder:       block.NewBlockBuilder(blockSize),
		blockMetaList:      block.NewBlockMetaList(),
		bloomFilterBuilder: bloom.NewBloomFilterBuilder(),
		blockSize:          blockSize,
	}
}

// Add adds the key/value pair in the current block builder.
// Add involves:
// 1) Keeping a track of the starting key and ending key of the current block.
// 2) Adding the key to the bloom.FilterBuilder
// 3) Adding the key/value pair to the current block.Builder.
// 4) Finishing the current block, if it is full and starting a new block (or block.Builder).
func (builder *SSTableBuilder) Add(key kv.Key, value kv.Value) {
	if builder.startingKey.IsRawKeyEmpty() {
		builder.startingKey = key
	}
	builder.endingKey = key
	builder.bloomFilterBuilder.Add(key)
	if builder.blockBuilder.Add(key, value) {
		return
	}
	builder.finishBlock()
	builder.startNewBlockBuilder(key)
	builder.blockBuilder.Add(key, value)
}

// Build builds the SSTable using the given id and file path.
// It involves encoding the SSTable, writing the entire table to persistent storage and creating an in-memory representation
// in the form of SSTable with a reference to its File.
// The encoding looks like:
/**
  ----------------------------------------------------------------------------------------------------------------------------------------------------------
| data block | data block |...| data block | metadata section | 4 bytes for meta starting offset | bloom filter section | 4 bytes for bloom starting offset |
|										   |				  |									 |					                                        |
 ----------------------------------------------------------------------------------------------------------------------------------------------------------
*/
func (builder *SSTableBuilder) Build(id uint64, filePath string) (SSTable, error) {
	blockMetaBeginOffset := func() []byte {
		blockMetaBeginOffset := make([]byte, block.Uint32Size)
		binary.LittleEndian.PutUint32(blockMetaBeginOffset, uint32(len(builder.allBlocksData)))
		return blockMetaBeginOffset
	}
	bloomOffset := func(buffer *bytes.Buffer) []byte {
		bloomOffset := make([]byte, block.Uint32Size)
		binary.LittleEndian.PutUint32(bloomOffset, uint32(buffer.Len()))
		return bloomOffset
	}

	builder.finishBlock()
	buffer := new(bytes.Buffer)
	buffer.Write(builder.allBlocksData)          //data blocks
	buffer.Write(builder.blockMetaList.Encode()) //metadata section block.MetaList.Encode()
	buffer.Write(blockMetaBeginOffset())         //4 bytes to indicate where the meta section starts from
	filter := builder.bloomFilterBuilder.Build(bloom.FalsePositiveRate)
	encodedFilter, err := filter.Encode()
	if err != nil {
		return SSTable{}, err
	}

	bloomFilterOffset := bloomOffset(buffer)
	buffer.Write(encodedFilter)     //bloom filter section bloom.Filter.Encode()
	buffer.Write(bloomFilterOffset) //4 bytes to indicate where the bloom filter section starts from

	file, err := Create(filePath, buffer.Bytes())
	if err != nil {
		return SSTable{}, err
	}

	startingKey, _ := builder.blockMetaList.StartingKeyOfFirstBlock()
	endingKey, _ := builder.blockMetaList.EndingKeyOfLastBlock()
	return SSTable{
		id:                   id,
		file:                 file,
		blockMetaList:        builder.blockMetaList,
		bloomFilter:          filter,
		blockMetaBeginOffset: uint32(len(builder.allBlocksData)),
		blockSize:            builder.blockSize,
		startingKey:          startingKey,
		endingKey:            endingKey,
	}, nil
}

// EstimatedSize returns an estimate of the size of the encoded data of all the blocks.
func (builder SSTableBuilder) EstimatedSize() int {
	return len(builder.allBlocksData)
}

// finishBlock finishes the current block. It involves:
// 1) Encoding the current block.
// 2) Storing the block.Meta in the block meta-list.
// 3) Collecting the encoded data of the current block in allBlocksData.
func (builder *SSTableBuilder) finishBlock() {
	encodedBlock := builder.blockBuilder.Build().Encode()
	builder.blockMetaList.Add(block.Meta{
		BlockStartingOffset: uint32(len(builder.allBlocksData)),
		StartingKey:         builder.startingKey,
		EndingKey:           builder.endingKey,
	})
	builder.allBlocksData = append(builder.allBlocksData, encodedBlock...)
}

// startNewBlockBuilder creates a new instance of SSTableBuilder.
func (builder *SSTableBuilder) startNewBlockBuilder(key kv.Key) {
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	builder.startingKey = key
	builder.endingKey = key
}
