package block

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate"), EndingKey: txn.NewStringKey("consensus")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 1, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
}

func TestBlockMetaListWithAThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate"), EndingKey: txn.NewStringKey("badger")})
	blockMetaList.Add(Meta{BlockStartingOffset: 4096, StartingKey: txn.NewStringKey("bolt"), EndingKey: txn.NewStringKey("calculator")})
	blockMetaList.Add(Meta{BlockStartingOffset: 8192, StartingKey: txn.NewStringKey("consensus"), EndingKey: txn.NewStringKey("distributed")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
}

func TestBlockMetaListWithAThreeBlockMetaWithEndingKeyOfEachBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate"), EndingKey: txn.NewStringKey("amorphous")})
	blockMetaList.Add(Meta{BlockStartingOffset: 4096, StartingKey: txn.NewStringKey("bolt"), EndingKey: txn.NewStringKey("bunt")})
	blockMetaList.Add(Meta{BlockStartingOffset: 8192, StartingKey: txn.NewStringKey("consensus"), EndingKey: txn.NewStringKey("distributed")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, uint32(0), meta.BlockStartingOffset)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, "amorphous", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, uint32(4096), meta.BlockStartingOffset)
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, "bunt", meta.EndingKey.RawString())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, uint32(8192), meta.BlockStartingOffset)
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "distributed", meta.EndingKey.RawString())
}

func TestBlockMetaListWithStartingKeyOfFirstBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate"), EndingKey: txn.NewStringKey("badger")})
	blockMetaList.Add(Meta{BlockStartingOffset: 4096, StartingKey: txn.NewStringKey("bolt"), EndingKey: txn.NewStringKey("calculator")})
	blockMetaList.Add(Meta{BlockStartingOffset: 8192, StartingKey: txn.NewStringKey("consensus"), EndingKey: txn.NewStringKey("distributed")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	startingKeyOfFirstBlock, ok := decodedBlockMetaList.StartingKeyOfFirstBlock()
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringKey("accurate"), startingKeyOfFirstBlock)
}

func TestBlockMetaListWithEndingKeyOfLastBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate"), EndingKey: txn.NewStringKey("amorphous")})
	blockMetaList.Add(Meta{BlockStartingOffset: 4096, StartingKey: txn.NewStringKey("bolt"), EndingKey: txn.NewStringKey("bunt")})
	blockMetaList.Add(Meta{BlockStartingOffset: 8192, StartingKey: txn.NewStringKey("consensus"), EndingKey: txn.NewStringKey("distributed")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	endingKeyOfLastBlock, ok := decodedBlockMetaList.EndingKeyOfLastBlock()
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringKey("distributed"), endingKeyOfLastBlock)
}

func TestBlockMetaListGetBlockContainingTheKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKey("bolt")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("bolt"))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("accurate"))
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("exact"))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("consensus"))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("consensus")})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKey("distributed")})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKey("etcd")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKey("consensus"), EndingKey: txn.NewStringKey("demo")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "demo", meta.EndingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	for count := 10; count <= 100; count += 10 {
		key := fmt.Sprintf("key-%d", count)
		blockMetaList.Add(Meta{BlockStartingOffset: uint32(count), StartingKey: txn.NewStringKey(key)})
	}

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("key-55"))
	assert.Equal(t, "key-50", meta.StartingKey.RawString())
	assert.Equal(t, 4, index)
}
