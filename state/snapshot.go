package state

import "go-lsm/table"

type StorageStateSnapshot struct {
	L0SSTableIds []uint64
	Levels       []*Level
	SSTables     map[uint64]*table.SSTable
}

func (snapshot StorageStateSnapshot) SSTableIdsAt(level int) []uint64 {
	if level == 0 {
		return snapshot.L0SSTableIds
	}
	return snapshot.Levels[level-1].SSTableIds
}
