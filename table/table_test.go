package table

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.add(BlockMeta{offset: 0, startingKey: txn.NewStringKey("accurate")})

	encoded := blockMetaList.encode()
	decodedBlockMetaList := decodeToBlockMetaList(encoded)

	assert.Equal(t, 1, len(decodedBlockMetaList.list))
	assert.Equal(t, "accurate", decodedBlockMetaList.list[0].startingKey.String())
}

func TestBlockMetaListWithAThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.add(BlockMeta{offset: 0, startingKey: txn.NewStringKey("accurate")})
	blockMetaList.add(BlockMeta{offset: 4096, startingKey: txn.NewStringKey("bolt")})
	blockMetaList.add(BlockMeta{offset: 8192, startingKey: txn.NewStringKey("consensus")})

	encoded := blockMetaList.encode()
	decodedBlockMetaList := decodeToBlockMetaList(encoded)

	assert.Equal(t, 3, len(decodedBlockMetaList.list))
	assert.Equal(t, "accurate", decodedBlockMetaList.list[0].startingKey.String())
	assert.Equal(t, "bolt", decodedBlockMetaList.list[1].startingKey.String())
	assert.Equal(t, "consensus", decodedBlockMetaList.list[2].startingKey.String())
}
