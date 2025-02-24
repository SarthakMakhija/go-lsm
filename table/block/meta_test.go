package block

import (
	"fmt"
	"go-lsm/kv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockMetaListWithASingleBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("consensus", 5),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	assert.Equal(t, 1, decodedBlockMetaList.Length())

	meta, _ := decodedBlockMetaList.GetAt(0)
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
}

func TestBlockMetaListWithThreeBlockMeta(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("badger", 3),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKeyWithTimestamp("bolt", 5),
		EndingKey:           kv.NewStringKeyWithTimestamp("calculator", 6),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKeyWithTimestamp("consensus", 5),
		EndingKey:           kv.NewStringKeyWithTimestamp("distributed", 6),
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

func TestBlockMetaListWithThreeBlockMetaWithEndingKeyOfEachBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           kv.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:           kv.NewStringKeyWithTimestamp("distributed", 10),
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
		StartingKey:         kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("badger", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           kv.NewStringKeyWithTimestamp("calculator", 7),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKeyWithTimestamp("consensus", 8),
		EndingKey:           kv.NewStringKeyWithTimestamp("distributed", 10),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	startingKeyOfFirstBlock, ok := decodedBlockMetaList.StartingKeyOfFirstBlock()
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("accurate", 2), startingKeyOfFirstBlock)
}

func TestBlockMetaListWithEndingKeyOfLastBlock(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKeyWithTimestamp("accurate", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("amorphous", 5),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 4096,
		StartingKey:         kv.NewStringKeyWithTimestamp("bolt", 6),
		EndingKey:           kv.NewStringKeyWithTimestamp("bunt", 8),
	})
	blockMetaList.Add(Meta{
		BlockStartingOffset: 8192,
		StartingKey:         kv.NewStringKeyWithTimestamp("consensus", 9),
		EndingKey:           kv.NewStringKeyWithTimestamp("distributed", 10),
	})

	encoded := blockMetaList.Encode()
	decodedBlockMetaList := DecodeToBlockMetaList(encoded)

	endingKeyOfLastBlock, ok := decodedBlockMetaList.EndingKeyOfLastBlock()
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringKeyWithTimestamp("distributed", 10), endingKeyOfLastBlock)
}

func TestBlockMetaListGetBlockContainingTheKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 10)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 11)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("bolt", 11))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockContainingTheKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("accurate", 2))
	assert.Equal(t, "accurate", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockContainingTheKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("exact", 8))
	assert.Equal(t, "exact", meta.StartingKey.RawString())
	assert.Equal(t, 3, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey1(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 8)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("consensus", 6))
	assert.Equal(t, "bolt", meta.StartingKey.RawString())
	assert.Equal(t, 1, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey2(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("consensus", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("distributed", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("etcd", 6)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("contribute", 6))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey3(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{
		BlockStartingOffset: 0,
		StartingKey:         kv.NewStringKeyWithTimestamp("consensus", 2),
		EndingKey:           kv.NewStringKeyWithTimestamp("demo", 5),
	})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("contribute", 3))
	assert.Equal(t, "consensus", meta.StartingKey.RawString())
	assert.Equal(t, "demo", meta.EndingKey.RawString())
	assert.Equal(t, 0, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey4(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("group", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey5(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("yugabyte", 8))
	assert.Equal(t, "gossip", meta.StartingKey.RawString())
	assert.Equal(t, 5, index)
}

func TestBlockMetaListGetBlockWhichMayContainTheGivenKey6(t *testing.T) {
	blockMetaList := NewBlockMetaList()
	blockMetaList.Add(Meta{BlockStartingOffset: 0, StartingKey: kv.NewStringKeyWithTimestamp("accurate", 2)})
	blockMetaList.Add(Meta{BlockStartingOffset: 20, StartingKey: kv.NewStringKeyWithTimestamp("bolt", 3)})
	blockMetaList.Add(Meta{BlockStartingOffset: 40, StartingKey: kv.NewStringKeyWithTimestamp("db", 4)})
	blockMetaList.Add(Meta{BlockStartingOffset: 60, StartingKey: kv.NewStringKeyWithTimestamp("exact", 5)})
	blockMetaList.Add(Meta{BlockStartingOffset: 80, StartingKey: kv.NewStringKeyWithTimestamp("foundation", 6)})
	blockMetaList.Add(Meta{BlockStartingOffset: 100, StartingKey: kv.NewStringKeyWithTimestamp("gossip", 7)})

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("fixed", 6))
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
			StartingKey:         kv.NewStringKeyWithTimestamp(key, timestamp),
		})
	}

	meta, index := blockMetaList.MaybeBlockMetaContaining(kv.NewStringKeyWithTimestamp("key-55", 50))
	assert.Equal(t, "key-50", meta.StartingKey.RawString())
	assert.Equal(t, 4, index)
}
