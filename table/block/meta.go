package block

import (
	"bytes"
	"encoding/binary"
	"go-lsm/kv"
)

type Meta struct {
	BlockStartingOffset uint32
	StartingKey         kv.Key
	EndingKey           kv.Key
}

type MetaList struct {
	list []Meta
}

func NewBlockMetaList() *MetaList {
	return &MetaList{}
}

func (metaList *MetaList) Add(meta Meta) {
	metaList.list = append(metaList.list, meta)
}

func (metaList *MetaList) Encode() []byte {
	numberOfBlocks := make([]byte, Uint32Size)
	binary.LittleEndian.PutUint32(numberOfBlocks, uint32(len(metaList.list)))

	resultingBuffer := new(bytes.Buffer)
	resultingBuffer.Write(numberOfBlocks)

	for _, blockMeta := range metaList.list {
		buffer := make(
			[]byte,
			Uint32Size+
				ReservedKeySize+
				blockMeta.StartingKey.EncodedSizeInBytes()+
				ReservedKeySize+
				blockMeta.EndingKey.EncodedSizeInBytes(),
		)

		binary.LittleEndian.PutUint32(buffer[:], blockMeta.BlockStartingOffset)

		binary.LittleEndian.PutUint16(buffer[Uint32Size:], uint16(blockMeta.StartingKey.EncodedSizeInBytes()))
		copy(buffer[Uint32Size+ReservedKeySize:], blockMeta.StartingKey.EncodedBytes())

		binary.LittleEndian.PutUint16(
			buffer[Uint32Size+ReservedKeySize+blockMeta.StartingKey.EncodedSizeInBytes():],
			uint16(blockMeta.EndingKey.EncodedSizeInBytes()),
		)
		copy(
			buffer[Uint32Size+ReservedKeySize+blockMeta.StartingKey.EncodedSizeInBytes()+ReservedKeySize:],
			blockMeta.EndingKey.EncodedBytes(),
		)
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

func (metaList *MetaList) MaybeBlockMetaContaining(key kv.Key) (Meta, int) {
	low, high := 0, metaList.Length()-1
	possibleIndex := low
	for low <= high {
		mid := low + (high-low)/2
		meta := metaList.list[mid]
		switch key.CompareKeysWithDescendingTimestamp(meta.StartingKey) {
		case -1:
			high = mid - 1
		case 0:
			return meta, mid
		case 1:
			possibleIndex = mid
			low = mid + 1
		}
	}
	return metaList.list[possibleIndex], possibleIndex
}

func DecodeToBlockMetaList(buffer []byte) *MetaList {
	numberOfBlocks := binary.LittleEndian.Uint32(buffer[:])
	blockList := make([]Meta, 0, numberOfBlocks)

	buffer = buffer[Uint32Size:]
	for blockCount := 0; blockCount < int(numberOfBlocks); blockCount++ {
		offset := binary.LittleEndian.Uint32(buffer[:])

		startingKeySize := binary.LittleEndian.Uint16(buffer[Uint32Size:])
		startingKeyBegin := 0 + Uint32Size + ReservedKeySize
		startingKey := buffer[startingKeyBegin : startingKeyBegin+int(startingKeySize)]

		endKeyBegin := 0 + startingKeyBegin + int(startingKeySize)
		endingKeySize := binary.LittleEndian.Uint16(buffer[endKeyBegin:])

		endKeyBegin = endKeyBegin + ReservedKeySize
		endingKey := buffer[endKeyBegin : endKeyBegin+int(endingKeySize)]

		blockList = append(blockList, Meta{
			BlockStartingOffset: offset,
			StartingKey:         kv.DecodeFrom(startingKey),
			EndingKey:           kv.DecodeFrom(endingKey),
		})
		index := endKeyBegin + int(endingKeySize)
		buffer = buffer[index:]
	}
	return &MetaList{
		list: blockList,
	}
}

func (metaList *MetaList) StartingKeyOfFirstBlock() (kv.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[0].StartingKey, true
	}
	return kv.Key{}, false
}

func (metaList *MetaList) EndingKeyOfLastBlock() (kv.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[metaList.Length()-1].EndingKey, true
	}
	return kv.Key{}, false
}
