package block

import (
	"bytes"
	"encoding/binary"
	"go-lsm/txn"
)

type Meta struct {
	Offset      uint32
	StartingKey txn.Key
	EndingKey   txn.Key
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
		buffer := make(
			[]byte,
			Uint32Size+
				ReservedKeySize+
				blockMeta.StartingKey.EncodedSizeInBytes()+
				ReservedKeySize+
				blockMeta.EndingKey.EncodedSizeInBytes(),
		)

		binary.LittleEndian.PutUint32(buffer[:], blockMeta.Offset)

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

func (metaList *MetaList) MaybeBlockMetaContaining(key txn.Key) (Meta, int) {
	low, high := 0, metaList.Length()
	previousLow := low
	for low < high {
		mid := low + (high-low)/2
		meta := metaList.list[mid]
		switch key.Compare(meta.StartingKey) {
		case -1:
			high = mid - 1
		case 0:
			return meta, mid
		case 1:
			next := mid + 1
			low = mid
			if next < metaList.Length() && key.Compare(metaList.list[next].StartingKey) >= 0 {
				low = mid + 1
			}
			if low == previousLow {
				return metaList.list[low], low
			}
		}
	}
	return metaList.list[low], low
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
			Offset:      offset,
			StartingKey: txn.DecodeFrom(startingKey),
			EndingKey:   txn.DecodeFrom(endingKey),
		})
		index := endKeyBegin + int(endingKeySize)
		buffer = buffer[index:]
	}
	return &MetaList{
		list: blockList,
	}
}

func (metaList *MetaList) StartingKeyOfFirstBlock() (txn.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[0].StartingKey, true
	}
	return txn.Key{}, false
}

func (metaList *MetaList) EndingKeyOfLastBlock() (txn.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[metaList.Length()-1].EndingKey, true
	}
	return txn.Key{}, false
}
