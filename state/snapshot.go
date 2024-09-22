package state

import "go-lsm/table"

// StorageStateSnapshot represents a point-in-time state of StorageState.
// It is obtained before compaction runs by the compaction goroutine.
type StorageStateSnapshot struct {
	L0SSTableIds []uint64
	Levels       []*Level
	SSTables     map[uint64]*table.SSTable
}

// SSTableIdsAt returns the SSTableIds at the given level.
func (snapshot StorageStateSnapshot) SSTableIdsAt(level int) []uint64 {
	if level == 0 {
		return snapshot.L0SSTableIds
	}
	return snapshot.Levels[level-1].SSTableIds
}
