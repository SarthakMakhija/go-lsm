package state

import (
	"github.com/stretchr/testify/assert"
	"go-lsm/compact/meta"
	"testing"
)

func TestAllSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds(t *testing.T) {
	event := StorageStateChangeEvent{
		description: meta.SimpleLeveledCompactionDescription{
			UpperLevelSSTableIds: []uint64{1, 2, 3, 4},
		},
	}
	excludedSSTableIds := event.allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds([]uint64{1, 2, 3, 4, 5, 6})

	assert.Equal(t, []uint64{5, 6}, excludedSSTableIds)
}
