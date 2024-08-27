package table

import (
	"go-lsm/kv"
	"go-lsm/table/block"
)

// Iterator represents SSTable iterator.
// An SSTable consists of multiple data blocks, so blockIndex maintains the current block which is
// being iterated over.
// blockIterator is a pointer to the block.Iterator.
// Effectively, an SSTable Iterator is an iterator which iterates over the blocks of SSTable.
type Iterator struct {
	table         SSTable
	blockIndex    int
	blockIterator *block.Iterator
}

// Key returns the kv.Key from block.Iterator.
func (iterator *Iterator) Key() kv.Key {
	return iterator.blockIterator.Key()
}

// Value returns the kv.Value from block.Iterator.
func (iterator *Iterator) Value() kv.Value {
	return iterator.blockIterator.Value()
}

// IsValid returns true of the block.Iterator is valid.
func (iterator *Iterator) IsValid() bool {
	return iterator.blockIterator.IsValid()
}

// Next advance the block.Iterator to the next key/value within the current block, or
// move to the next block, if such a block exists.
func (iterator *Iterator) Next() error {
	if err := iterator.blockIterator.Next(); err != nil {
		return err
	}
	if !iterator.blockIterator.IsValid() {
		iterator.blockIndex += 1
		if iterator.blockIndex < iterator.table.noOfBlocks() {
			readBlock, err := iterator.table.readBlock(iterator.blockIndex)
			if err != nil {
				return err
			}
			iterator.blockIterator = readBlock.SeekToFirst()
		}
	}
	return nil
}

// Close does nothing.
func (iterator *Iterator) Close() {}
