package state

const totalLevels = 6

// Level represents a level in LSM.
// It does not include level0. SSTableIds of level0 are represented by the field "l0SSTableIds" in StorageState.
type Level struct {
	LevelNumber int
	SSTableIds  []uint64
}

// clearSSTableIds cleans the SSTableIds.
func (level *Level) clearSSTableIds() {
	level.SSTableIds = nil
}

// appendSSTableIds appends the new ssTableIds to the existing ssTableIds.
func (level *Level) appendSSTableIds(ssTableIds []uint64) {
	level.SSTableIds = append(level.SSTableIds, ssTableIds...)
}
