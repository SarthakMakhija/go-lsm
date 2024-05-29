package block

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 1, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.String())
}

func TestBlockMetaListWithAThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{Offset: 4096, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{Offset: 8192, StartingKey: txn.NewStringKey("consensus")})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 3, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.String())

	meta, _ = decodedBlockMetaList.GetAt(1)
	assert.Equal(t, "bolt", meta.StartingKey.String())

	meta, _ = decodedBlockMetaList.GetAt(2)
	assert.Equal(t, "consensus", meta.StartingKey.String())
}

func TestBlockMetaListGetBlockContainingTheKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{Offset: 20, StartingKey: txn.NewStringKey("bolt")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("bolt"))
	assert.Equal(t, "bolt", meta.StartingKey.String())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{Offset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{Offset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{Offset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("accurate"))
	assert.Equal(t, "accurate", meta.StartingKey.String())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{Offset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{Offset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{Offset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("exact"))
	assert.Equal(t, "exact", meta.StartingKey.String())
	assert.Equal(t, 3, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("accurate")})
	blockMetaList.Add(Meta{Offset: 20, StartingKey: txn.NewStringKey("bolt")})
	blockMetaList.Add(Meta{Offset: 40, StartingKey: txn.NewStringKey("db")})
	blockMetaList.Add(Meta{Offset: 60, StartingKey: txn.NewStringKey("exact")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("consensus"))
	assert.Equal(t, "bolt", meta.StartingKey.String())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("consensus")})
	blockMetaList.Add(Meta{Offset: 20, StartingKey: txn.NewStringKey("distributed")})
	blockMetaList.Add(Meta{Offset: 40, StartingKey: txn.NewStringKey("etcd")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.String())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{Offset: 0, StartingKey: txn.NewStringKey("consensus")})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("contribute"))
	assert.Equal(t, "consensus", meta.StartingKey.String())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	for count := 10; count <= 100; count += 10 {
		key := fmt.Sprintf("key-%d", count)
		blockMetaList.Add(Meta{Offset: uint32(count), StartingKey: txn.NewStringKey(key)})
	}

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKey("key-55"))
	assert.Equal(t, "key-50", meta.StartingKey.String())
	assert.Equal(t, 4, index)
}
