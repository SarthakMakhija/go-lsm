package table

import (
	"encoding/binary"
	"fmt"
	"go-lsm/table/block"
	"go-lsm/table/bloom"
	"go-lsm/txn"
)

type SSTable struct {
	id              uint64
	blockMetaList   *block.MetaList
	bloomFilter     bloom.Filter
	file            *File
	blockMetaOffset uint32
	blockSize       uint
	startingKey     txn.Key
	endingKey       txn.Key
}

func Load(id uint64, filePath string, blockSize uint) (SSTable, error) {
	file, err := Open(filePath)
	if err != nil {
		return SSTable{}, err
	}

	fileSize := file.Size()
	bloomFilter := func() (bloom.Filter, uint32, error) {
		offsetBuffer := make([]byte, block.Uint32Size)
		n, err := file.Read(fileSize-int64(block.Uint32Size), offsetBuffer)
		if err != nil {
			return bloom.Filter{}, 0, err
		}

		bloomOffset := binary.LittleEndian.Uint32(offsetBuffer[:n])
		bloomBuffer := make([]byte, fileSize-int64(bloomOffset)-int64(block.Uint32Size))
		n, err = file.Read(int64(bloomOffset), bloomBuffer)
		if err != nil {
			return bloom.Filter{}, 0, err
		}
		filter, err := bloom.DecodeToBloomFilter(bloomBuffer, bloom.FalsePositiveRate)
		return filter, bloomOffset, err
	}
	blockMetaList := func(bloomOffset uint32) (*block.MetaList, uint32, error) {
		blockMetaOffsetBuffer := make([]byte, block.Uint32Size)
		n, err := file.Read(int64(bloomOffset-uint32(block.Uint32Size)), blockMetaOffsetBuffer)
		if err != nil {
			return nil, 0, err
		}

		blockMetaOffset := binary.LittleEndian.Uint32(blockMetaOffsetBuffer[:n])
		blockMetaListBuffer := make([]byte, int64(bloomOffset)-int64(block.Uint32Size))
		n, err = file.Read(int64(blockMetaOffset), blockMetaListBuffer)
		if err != nil {
			return nil, 0, err
		}
		return block.DecodeToBlockMetaList(blockMetaListBuffer), blockMetaOffset, nil
	}

	filter, bloomOffset, err := bloomFilter()
	if err != nil {
		return SSTable{}, err
	}
	metaList, metaOffset, err := blockMetaList(bloomOffset)
	if err != nil {
		return SSTable{}, err
	}
	startingKey, _ := metaList.StartingKeyOfFirstBlock()
	endingKey, _ := metaList.EndingKeyOfLastBlock()
	return SSTable{
		id:              id,
		blockMetaList:   metaList,
		bloomFilter:     filter,
		blockMetaOffset: metaOffset,
		file:            file,
		blockSize:       blockSize,
		startingKey:     startingKey,
		endingKey:       endingKey,
	}, nil
}

func (table SSTable) SeekToFirst() (*Iterator, error) {
	readBlock, err := table.readBlock(0)
	if err != nil {
		return nil, err
	}
	return &Iterator{
		table:         table,
		blockIndex:    0,
		blockIterator: readBlock.SeekToFirst(),
	}, nil
}

func (table SSTable) SeekToKey(key txn.Key) (*Iterator, error) {
	_, blockIndex := table.blockMetaList.MaybeBlockMetaContaining(key)
	readBlock, err := table.readBlock(blockIndex)
	if err != nil {
		return nil, err
	}
	blockIterator := readBlock.SeekToKey(key)
	if !blockIterator.IsValid() {
		blockIndex += 1
		if blockIndex < table.noOfBlocks() {
			readBlock, err := table.readBlock(blockIndex)
			if err != nil {
				return nil, err
			}
			blockIterator = readBlock.SeekToKey(key)
		}
	}
	return &Iterator{
		table:         table,
		blockIndex:    blockIndex,
		blockIterator: blockIterator,
	}, nil
}

func (table SSTable) ContainsInclusive(inclusiveKeyRange txn.InclusiveKeyRange) bool {
	if inclusiveKeyRange.Start().CompareKeysWithDescendingTimestamp(table.endingKey) > 0 {
		return false
	}
	if inclusiveKeyRange.End().CompareKeysWithDescendingTimestamp(table.startingKey) < 0 {
		return false
	}
	return true
}

func (table SSTable) MayContain(key txn.Key) bool {
	return table.bloomFilter.MayContain(key)
}

func (table SSTable) Id() uint64 {
	return table.id
}

func (table SSTable) readBlock(blockIndex int) (block.Block, error) {
	startingOffset, endOffset := table.offsetRangeOfBlockAt(blockIndex)
	buffer := make([]byte, endOffset-startingOffset)
	n, err := table.file.Read(int64(startingOffset), buffer)
	if err != nil {
		return block.Block{}, err
	}
	return block.DecodeToBlock(buffer[:n]), nil
}

func (table SSTable) noOfBlocks() int {
	return table.blockMetaList.Length()
}

func (table SSTable) offsetRangeOfBlockAt(blockIndex int) (uint32, uint32) {
	blockMeta, ok := table.blockMetaList.GetAt(blockIndex)
	if !ok {
		panic(fmt.Errorf("block meta not found at index %v", blockIndex))
	}
	nextBlockMeta, ok := table.blockMetaList.GetAt(blockIndex + 1)

	var endOffset uint32
	if ok {
		endOffset = nextBlockMeta.BlockStartingOffset
	} else {
		endOffset = table.blockMetaOffset
	}
	return blockMeta.BlockStartingOffset, endOffset
}
