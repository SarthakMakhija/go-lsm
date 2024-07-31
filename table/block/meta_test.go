package block

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-lsm/txn"
	"testing"
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("consensus", 5),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 1, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
}

func TestBlockMetaListWithAThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("badger", 3),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         txn.NewStringKeyWithTimestamp("bolt", 5),
		EndingKey:           txn.NewStringKeyWithTimestamp("calculator", 6),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         txn.NewStringKeyWithTimestamp("consensus", 5),
		EndingKey:           txn.NewStringKeyWithTimestamp("distributed", 6),
	})

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
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         txn.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           txn.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         txn.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:           txn.NewStringKeyWithTimestamp("distributed", 10),
	})

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
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("badger", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         txn.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           txn.NewStringKeyWithTimestamp("calculator", 7),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         txn.NewStringKeyWithTimestamp("consensus", 8),
		EndingKey:           txn.NewStringKeyWithTimestamp("distributed", 10),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	startingKeyOfFirstBlock, ok := decodedBlockMetaList.StartingKeyOfFirstBlock()
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("accurate", 2), startingKeyOfFirstBlock)
}

func TestBlockMetaListWithEndingKeyOfLastBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         txn.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           txn.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         txn.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:           txn.NewStringKeyWithTimestamp("distributed", 10),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	endingKeyOfLastBlock, ok := decodedBlockMetaList.EndingKeyOfLastBlock()
	assert.True(t, ok)
	assert.Equal(t, txn.NewStringKeyWithTimestamp("distributed", 10), endingKeyOfLastBlock)
}

func TestBlockMetaListGetBlockContainingTheKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 10)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 11)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("bolt", 11))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("accurate", 2))
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("exact", 8))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("consensus", 6))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("consensus", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("distributed", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("etcd", 6)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("contribute", 6))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         txn.NewStringKeyWithTimestamp("consensus", 2),
		EndingKey:           txn.NewStringKeyWithTimestamp("demo", 5),
	})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("contribute", 3))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "demo", meta.EndingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: txn.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: txn.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("group", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey5(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: txn.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: txn.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("yugabyte", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey6(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: txn.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: txn.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: txn.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: txn.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: txn.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: txn.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("fixed", 6))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey7(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	for count := 10; count <= 100; count += 10 {
		key := fmt.Sprintf("key-%d", count)
		timestamp := uint64(count)
		blockMetaList.Add(Meta{
			BlockStartingOffset: uint32(count),
			StartingKey:         txn.NewStringKeyWithTimestamp(key, timestamp),
		})
	}

	meta, index := blockMetaList.MaybeBlockMetaContaining(txn.NewStringKeyWithTimestamp("key-55", 50))
	assert.Equal(t, "key-50", meta.StartingKey.RawString())
	assert.Equal(t, 4, index)
}
