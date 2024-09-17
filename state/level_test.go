package state

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClearSStableIds(t *testing.T) {
	level := &Level{LevelNumber: 1, SSTableIds: []uint64{1, 2, 3}}
	level.clearSSTableIds()

	assert.Equal(t, []uint64(nil), level.SSTableIds)
}

func TestAppendSStableIds(t *testing.T) {
	level := &Level{LevelNumber: 1, SSTableIds: []uint64{1, 2, 3}}
	level.appendSSTableIds([]uint64{4, 5})

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, level.SSTableIds)
}
