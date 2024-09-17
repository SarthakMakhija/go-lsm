package state

const totalLevels = 6

type Level struct {
	LevelNumber int
	SSTableIds  []uint64
}

func (level *Level) clearSSTableIds() {
	level.SSTableIds = nil
}

func (level *Level) appendSSTableIds(ssTableIds []uint64) {
	level.SSTableIds = append(level.SSTableIds, ssTableIds...)
}
