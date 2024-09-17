package state

import "go-lsm/table"

type StorageStateSnapshot struct {
	L0SSTableIds []uint64
	Levels       []*Level
	SSTables     map[uint64]table.SSTable
}

func (snapshot StorageStateSnapshot) OrderedSSTableIds(level int) []uint64 {
	if level == 0 {
		ids := make([]uint64, 0, len(snapshot.L0SSTableIds))
		for l0SSTableIndex := len(snapshot.L0SSTableIds) - 1; l0SSTableIndex >= 0; l0SSTableIndex-- {
			ids = append(ids, snapshot.L0SSTableIds[l0SSTableIndex])
		}
		return ids
	}
	ssTableIds := snapshot.Levels[level-1].SSTableIds
	ids := make([]uint64, 0, len(ssTableIds))
	for ssTableIndex := len(ssTableIds) - 1; ssTableIndex >= 0; ssTableIndex-- {
		ids = append(ids, ssTableIds[ssTableIndex])
	}
	return ids
}
