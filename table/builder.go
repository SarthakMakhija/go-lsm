package table

import (
	"bytes"
	"encoding/binary"
	"go-lsm/kv"
	"go-lsm/table/block"
	"go-lsm/table/bloom"
)

type SSTableBuilder struct {
	blockBuilder       *block.Builder
	blockMetaList      *block.MetaList
	bloomFilterBuilder *bloom.FilterBuilder
	startingKey        kv.Key
	endingKey          kv.Key
	allBlocksData      []byte
	blockSize          uint
}

func NewSSTableBuilderWithDefaultBlockSize() *SSTableBuilder {
	return NewSSTableBuilder(block.DefaultBlockSize)
}

func NewSSTableBuilder(blockSize uint) *SSTableBuilder {
	return &SSTableBuilder{
		blockBuilder:       block.NewBlockBuilder(blockSize),
		blockMetaList:      block.NewBlockMetaList(),
		bloomFilterBuilder: bloom.NewBloomFilterBuilder(),
		blockSize:          blockSize,
	}
}

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
	builder.startNewBlock(key)
	builder.blockBuilder.Add(key, value)
}

// Build
// TODO: Bloom
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
	buffer.Write(builder.allBlocksData)
	buffer.Write(builder.blockMetaList.Encode())
	buffer.Write(blockMetaBeginOffset())
	filter := builder.bloomFilterBuilder.Build(bloom.FalsePositiveRate)
	encodedFilter, err := filter.Encode()
	if err != nil {
		return SSTable{}, err
	}

	bloomFilterOffset := bloomOffset(buffer)
	buffer.Write(encodedFilter)
	buffer.Write(bloomFilterOffset)

	file, err := Create(filePath, buffer.Bytes())
	if err != nil {
		return SSTable{}, err
	}
	//TODO: Block cache + bloom fields

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

func (builder SSTableBuilder) EstimatedSize() int {
	return len(builder.allBlocksData)
}

func (builder *SSTableBuilder) finishBlock() {
	encodedBlock := builder.blockBuilder.Build().Encode()
	builder.blockMetaList.Add(block.Meta{
		BlockStartingOffset: uint32(len(builder.allBlocksData)),
		StartingKey:         builder.startingKey,
		EndingKey:           builder.endingKey,
	})
	builder.allBlocksData = append(builder.allBlocksData, encodedBlock...)
}

func (builder *SSTableBuilder) startNewBlock(key kv.Key) {
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	builder.startingKey = key
	builder.endingKey = key
}
