package compaction

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/kv"
	"go-lsm/state"
	"go-lsm/test_utility"
	"go-lsm/txn"
	"testing"
)

func TestGenerateSSTablesFromASingleIterator(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	iterator := newMockIterator(
		[]kv.Key{
			kv.NewStringKeyWithTimestamp("consensus", 11),
			kv.NewStringKeyWithTimestamp("storage", 11),
		},
		[]kv.Value{
			kv.NewStringValue("VSR"),
			kv.NewStringValue("NVMe"),
		},
	)

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageState.Options())
	ssTables, err := compaction.ssTablesFromIterator(iterator)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(ssTables))

	ssTable := ssTables[0]
	ssTableIterator, err := ssTable.SeekToKey(kv.NewStringKeyWithTimestamp("consensus", 11))

	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringValue("VSR"), ssTableIterator.Value())

	assert.Nil(t, ssTableIterator.Next())
	assert.Equal(t, kv.NewStringValue("NVMe"), ssTableIterator.Value())
}

func TestGenerateSSTablesFromASingleIteratorHavingMultipleKeysWithDifferentTimestampsWhichAreEligibleToBeDiscarded(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	iterator := newMockIterator(
		[]kv.Key{
			kv.NewStringKeyWithTimestamp("consensus", 11),
			kv.NewStringKeyWithTimestamp("consensus", 10),
			kv.NewStringKeyWithTimestamp("consensus", 9),
		},
		[]kv.Value{
			kv.NewStringValue("VSR"),
			kv.NewStringValue("Paxos"),
			kv.NewStringValue("Raft"),
		},
	)

	oracle.SetBeginTimestamp(11)

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageState.Options())
	ssTables, err := compaction.ssTablesFromIterator(iterator)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(ssTables))

	ssTable := ssTables[0]
	ssTableIterator, err := ssTable.SeekToFirst()

	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringValue("VSR"), ssTableIterator.Value())

	assert.Nil(t, ssTableIterator.Next())
	assert.False(t, ssTableIterator.IsValid())
}

func TestGenerateSSTablesFromASingleIteratorHavingMultipleKeysWithDifferentTimestampsSuchThatOneOfTheKeysHasATimestampGreaterThanTheMaxBeginTimestamp(t *testing.T) {
	rootPath := test_utility.SetupADirectoryWithTestName(t)
	storageState, _ := state.NewStorageState(rootPath)
	oracle := txn.NewOracle(txn.NewExecutor(storageState))

	defer func() {
		test_utility.CleanupDirectoryWithTestName(t)
		storageState.Close()
		oracle.Close()
	}()

	iterator := newMockIterator(
		[]kv.Key{
			kv.NewStringKeyWithTimestamp("consensus", 11),
			kv.NewStringKeyWithTimestamp("consensus", 10),
			kv.NewStringKeyWithTimestamp("consensus", 9),
		},
		[]kv.Value{
			kv.NewStringValue("VSR"),
			kv.NewStringValue("Paxos"),
			kv.NewStringValue("Raft"),
		},
	)

	oracle.SetBeginTimestamp(10)

	compaction := NewCompaction(oracle, storageState.SSTableIdGenerator(), storageState.Options())
	ssTables, err := compaction.ssTablesFromIterator(iterator)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(ssTables))

	ssTable := ssTables[0]
	ssTableIterator, err := ssTable.SeekToFirst()

	assert.Nil(t, err)
	assert.Equal(t, kv.NewStringValue("VSR"), ssTableIterator.Value())

	assert.Nil(t, ssTableIterator.Next())
	assert.Equal(t, kv.NewStringValue("Paxos"), ssTableIterator.Value())

	assert.Nil(t, ssTableIterator.Next())
	assert.False(t, ssTableIterator.IsValid())
}
