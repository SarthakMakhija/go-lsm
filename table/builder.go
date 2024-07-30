package table

import (
	"bytes"
	"encoding/binary"
	"go-lsm/table/block"
	"go-lsm/table/bloom"
	"go-lsm/txn"
)

type SSTableBuilder struct {
	blockBuilder       *block.Builder
	blockMetaList      *block.MetaList
	bloomFilterBuilder *bloom.FilterBuilder
	startingKey        txn.Key
	endingKey          txn.Key
	blocksData         []byte
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

func (builder *SSTableBuilder) Add(key txn.Key, value txn.Value) {
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
	blockMetaOffset := func() []byte {
		blockMetaOffset := make([]byte, block.Uint32Size)
		binary.LittleEndian.PutUint32(blockMetaOffset, uint32(len(builder.blocksData)))
		return blockMetaOffset
	}
	bloomOffset := func(buffer *bytes.Buffer) []byte {
		bloomOffset := make([]byte, block.Uint32Size)
		binary.LittleEndian.PutUint32(bloomOffset, uint32(buffer.Len()))
		return bloomOffset
	}

	builder.finishBlock()
	buffer := new(bytes.Buffer)
	buffer.Write(builder.blocksData)
	buffer.Write(builder.blockMetaList.Encode())
	buffer.Write(blockMetaOffset())
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
		id:              id,
		file:            file,
		blockMetaList:   builder.blockMetaList,
		bloomFilter:     filter,
		blockMetaOffset: uint32(len(builder.blocksData)),
		blockSize:       builder.blockSize,
		startingKey:     startingKey,
		endingKey:       endingKey,
	}, nil
}

func (builder SSTableBuilder) EstimatedSize() int {
	return len(builder.blocksData)
}

func (builder *SSTableBuilder) finishBlock() {
	encodedBlock := builder.blockBuilder.Build().Encode()
	builder.blockMetaList.Add(block.Meta{
		Offset:      uint32(len(builder.blocksData)),
		StartingKey: builder.startingKey,
		EndingKey:   builder.endingKey,
	})
	builder.blocksData = append(builder.blocksData, encodedBlock...)
}

func (builder *SSTableBuilder) startNewBlock(key txn.Key) {
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	builder.startingKey = key
	builder.endingKey = key
}
