package block

import (
	"bytes"
	"encoding/binary"
	"go-lsm/txn"
)

type Meta struct {
	Offset      uint32
	StartingKey txn.Key
}

type MetaList struct {
	list []Meta
}

func NewBlockMetaList() *MetaList {
	return &MetaList{}
}

func (metaList *MetaList) Add(block Meta) {
	metaList.list = append(metaList.list, block)
}

func (metaList *MetaList) Encode() []byte {
	numberOfBlocks := make([]byte, Uint32Size)
	binary.LittleEndian.PutUint32(numberOfBlocks, uint32(len(metaList.list)))

	resultingBuffer := new(bytes.Buffer)
	resultingBuffer.Write(numberOfBlocks)

	for _, blockMeta := range metaList.list {
		buffer := make([]byte, Uint32Size+ReservedKeySize+blockMeta.StartingKey.Size())

		binary.LittleEndian.PutUint32(buffer[:], blockMeta.Offset)
		binary.LittleEndian.PutUint16(buffer[Uint32Size:], uint16(blockMeta.StartingKey.Size()))
		copy(buffer[Uint32Size+ReservedKeySize:], blockMeta.StartingKey.Bytes())

		resultingBuffer.Write(buffer)
	}

	return resultingBuffer.Bytes()
}

func (metaList *MetaList) GetAt(index int) (Meta, bool) {
	if index < len(metaList.list) {
		return metaList.list[index], true
	}
	return Meta{}, false
}

func (metaList *MetaList) Length() int {
	return len(metaList.list)
}

func DecodeToBlockMetaList(buffer []byte) MetaList {
	numberOfBlocks := binary.LittleEndian.Uint32(buffer[:])
	blockList := make([]Meta, 0, numberOfBlocks)

	buffer = buffer[Uint32Size:]
	for index := 0; index < len(buffer); {
		offset := binary.LittleEndian.Uint32(buffer[index:])
		keySize := binary.LittleEndian.Uint16(buffer[index+Uint32Size:])
		key := buffer[index+Uint32Size+ReservedKeySize : index+Uint32Size+ReservedKeySize+int(keySize)]

		blockList = append(blockList, Meta{
			Offset:      offset,
			StartingKey: txn.NewKey(key),
		})
		index = index + Uint32Size + ReservedKeySize + int(keySize)
	}
	return MetaList{
		list: blockList,
	}
}
