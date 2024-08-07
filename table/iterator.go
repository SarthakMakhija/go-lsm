package table

import (
	"go-lsm/kv"
	"go-lsm/table/block"
)

type Iterator struct {
	table         SSTable
	blockIndex    int
	blockIterator *block.Iterator
}

func (iterator *Iterator) Key() kv.Key {
	return iterator.blockIterator.Key()
}

func (iterator *Iterator) Value() kv.Value {
	return iterator.blockIterator.Value()
}

func (iterator *Iterator) IsValid() bool {
	return iterator.blockIterator.IsValid()
}

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

func (iterator *Iterator) Close() {}
