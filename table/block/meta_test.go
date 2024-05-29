package block

import (
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
