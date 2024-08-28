package table

import (
	"encoding/binary"
	"fmt"
	"go-lsm/kv"
	"go-lsm/table/block"
	"go-lsm/table/bloom"
)

// SSTable represents SSTable on disk.
type SSTable struct {
	id                    uint64
	blockMetaList         *block.MetaList
	bloomFilter           bloom.Filter
	file                  *File
	blockMetaOffsetMarker uint32
	blockSize             uint
	startingKey           kv.Key
	endingKey             kv.Key
}

// Load loads the entire SSTable from the given filePath.
// Please take a look at table.SSTableBuilder to understand the encoding of SSTable.
func Load(id uint64, filePath string, blockSize uint) (SSTable, error) {
	file, err := Open(filePath)
	if err != nil {
		return SSTable{}, err
	}

	fileSize := file.Size()

	//bloomFilter reads the bloom filter section in the file. It involves the following:
	// 1) Read the last 4 bytes to get the starting offset of the bloom filter. Let's call this offset as X.
	// 2) Read the buffer of size (fileSize - X - 4 bytes) from offset X.
	// 3) Decode the buffer to bloom filter.
	// Let's consider that the file size is 1024 bytes and the last 4 bytes denote the starting position (/offset) of the
	// bloom filter. Let's say that the last 4 bytes contain the offset as 996. That means, the byte buffer from offset
	// 996 to (1024 - 4) [24 bytes] is the bloom filter buffer => 1024 - 996 - 4 is same as fileSize - X - 4 bytes.
	// This means, read 24 bytes after seeking to offset X in the file.
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
	//blockMetaList reads the block meta-list from the file. It involves the following:
	// 1) Read the 4 bytes before the bloom filter section to get the starting offset of the meta section. Let's call this offset as Y.
	// 2) Read the buffer of size (bloom offset - 4 bytes) from offset Y.
	// 3) Decode the buffer to meta-list.
	// Let's consider that the file size is 1024 bytes and the last 4 bytes denote the starting position (/offset) of the
	// bloom filter. Let's say that the last 4 bytes contain the offset as 996.
	// Also, consider that the 4 bytes before the bloom section contain 900 as the starting offset of metadata section.
	// This means, read a total of 992-900 (92) bytes from offset 900 will give the meta-list buffer.
	// 996 if the starting offset of bloom filter section, minus 4 bytes which contain the meta-list starting offset gives us
	// offset 992. So, reading from the offset 900 (the starting offset of metadata section) to 992 gives us meta-list buffer.
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
		id:                    id,
		blockMetaList:         metaList,
		bloomFilter:           filter,
		blockMetaOffsetMarker: metaOffset,
		file:                  file,
		blockSize:             blockSize,
		startingKey:           startingKey,
		endingKey:             endingKey,
	}, nil
}

// SeekToFirst seeks to the first key in the SSTable.
// First key is a part of the first block, so the block at index 0 is read and a block.Iterator
// is created over the read block.
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

// SeekToKey seeks to the block that contains a key greater than or equal to the given key.
// It involves the following:
// 1) Identify the block.Meta that may contain the key.
// 2) Read the block identified by blockIndex.
// 3) Seek to the key within the read block (seeks to the offset where the key >= the given key)
// 4) Handle the case where block.Iterator may become invalid.
func (table SSTable) SeekToKey(key kv.Key) (*Iterator, error) {
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

// ContainsInclusive returns true if the SSTable contains the inclusiveKeyRange.
// It returns false:
// If the starting key of the inclusiveKeyRange is greater than the ending key of the SSTable, Or
// If the ending key of the inclusiveKeyRange is less than the starting key of the SSTable.
// Returns true otherwise.
func (table SSTable) ContainsInclusive(inclusiveKeyRange kv.InclusiveKeyRange[kv.Key]) bool {
	if inclusiveKeyRange.Start().CompareKeysWithDescendingTimestamp(table.endingKey) > 0 {
		return false
	}
	if inclusiveKeyRange.End().CompareKeysWithDescendingTimestamp(table.startingKey) < 0 {
		return false
	}
	return true
}

// MayContain uses bloom filter to determine if the given key maybe present in the SSTable.
// Returns true if the key MAYBE present, false otherwise.
func (table SSTable) MayContain(key kv.Key) bool {
	return table.bloomFilter.MayContain(key)
}

// Id returns the id of SSTable.
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

// noOfBlocks returns the number of blocks in SSTable.
func (table SSTable) noOfBlocks() int {
	return table.blockMetaList.Length()
}

// offsetRangeOfBlockAt returns the byte offset range of the block at the given index.
// offsetRangeOfBlockAt works by getting the block.Meta at the given index, and block.Meta at index + 1.
// If the block.Meta is available at the next index, it returns the BlockStartingOffset of block.Meta at the given index,
// and block.Meta at index + 1.
// If the block.Meta is not available at the next index, it returns the BlockStartingOffset of block.Meta at the given index,
// and table.blockMetaOffsetMarker, which is essentially the offset of the 4-bytes which denote the meta starting offset.
// Please take a look at the table.SSTableBuilder for encoding of SSTable.
func (table SSTable) offsetRangeOfBlockAt(blockIndex int) (uint32, uint32) {
	blockMeta, blockPresent := table.blockMetaList.GetAt(blockIndex)
	if !blockPresent {
		panic(fmt.Errorf("block meta not found at index %v", blockIndex))
	}
	nextBlockMeta, nextBlockPresent := table.blockMetaList.GetAt(blockIndex + 1)

	var endOffset uint32
	if nextBlockPresent {
		endOffset = nextBlockMeta.BlockStartingOffset
	} else {
		endOffset = table.blockMetaOffsetMarker
	}
	return blockMeta.BlockStartingOffset, endOffset
}
