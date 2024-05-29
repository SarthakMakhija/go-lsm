package table

import (
	"go-lsm/table/block"
)

type SSTable struct {
	id              uint64
	blockMetaList   *block.MetaList
	blockMetaOffset uint32
	file            *File
	blockSize       uint
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
		panic("block meta not found")
	}
	nextBlockMeta, ok := table.blockMetaList.GetAt(blockIndex + 1)
	var endOffset uint32
	if ok {
		endOffset = nextBlockMeta.Offset
	} else {
		endOffset = table.blockMetaOffset
	}
	return blockMeta.Offset, endOffset
}
