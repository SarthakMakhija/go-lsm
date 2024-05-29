package table

import (
	"encoding/binary"
	"go-lsm/table/block"
	"go-lsm/txn"
)

type SSTable struct {
	id              uint64
	blockMetaList   *block.MetaList
	file            *File
	blockMetaOffset uint32
	blockSize       uint
}

func Load(id uint64, filePath string, blockSize uint) (SSTable, error) {
	file, err := Open(filePath)
	if err != nil {
		return SSTable{}, err
	}

	fileSize := file.Size()
	blockMetaOffsetBuffer := make([]byte, block.Uint32Size)
	n, err := file.Read(fileSize-int64(block.Uint32Size), blockMetaOffsetBuffer)
	if err != nil {
		return SSTable{}, err
	}
	blockMetaOffset := binary.LittleEndian.Uint32(blockMetaOffsetBuffer[:n])

	blockMetaListBuffer := make([]byte, fileSize-int64(blockMetaOffset)-int64(block.Uint32Size))
	n, err = file.Read(int64(blockMetaOffset), blockMetaListBuffer)
	if err != nil {
		return SSTable{}, err
	}
	return SSTable{
		id:              id,
		blockMetaList:   block.DecodeToBlockMetaList(blockMetaListBuffer),
		blockMetaOffset: blockMetaOffset,
		file:            file,
		blockSize:       blockSize,
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
