package table

import (
	"bytes"
	"encoding/binary"
	"go-lsm/txn"
)

type BlockMeta struct {
	offset      uint
	startingKey txn.Key
}

type BlockMetaList struct {
	list []BlockMeta
}

func NewBlockMetaList() *BlockMetaList {
	return &BlockMetaList{}
}

func (metaList *BlockMetaList) add(block BlockMeta) {
	metaList.list = append(metaList.list, block)
}

func (metaList *BlockMetaList) encode() []byte {
	numberOfBlocks := make([]byte, uint32Size)
	binary.LittleEndian.PutUint32(numberOfBlocks, uint32(len(metaList.list)))

	resultingBuffer := new(bytes.Buffer)
	resultingBuffer.Write(numberOfBlocks)

	for _, blockMeta := range metaList.list {
		buffer := make([]byte, uint32Size+reservedKeySize+blockMeta.startingKey.Size())

		binary.LittleEndian.PutUint32(buffer[:], uint32(blockMeta.offset))
		binary.LittleEndian.PutUint16(buffer[uint32Size:], uint16(blockMeta.startingKey.Size()))
		copy(buffer[uint32Size+reservedKeySize:], blockMeta.startingKey.Bytes())

		resultingBuffer.Write(buffer)
	}

	return resultingBuffer.Bytes()
}

func decodeToBlockMetaList(buffer []byte) BlockMetaList {
	numberOfBlocks := binary.LittleEndian.Uint32(buffer[:])
	blockList := make([]BlockMeta, 0, numberOfBlocks)

	buffer = buffer[uint32Size:]
	for index := 0; index < len(buffer); {
		offset := binary.LittleEndian.Uint32(buffer[index:])
		keySize := binary.LittleEndian.Uint16(buffer[index+uint32Size:])
		key := buffer[index+uint32Size+reservedKeySize : index+uint32Size+reservedKeySize+int(keySize)]

		blockList = append(blockList, BlockMeta{
			offset:      uint(offset),
			startingKey: txn.NewKey(key),
		})
		index = index + uint32Size + reservedKeySize + int(keySize)
	}
	return BlockMetaList{
		list: blockList,
	}
}
