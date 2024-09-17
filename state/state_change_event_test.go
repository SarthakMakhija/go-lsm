package state

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds(t *testing.T) {
	event := StorageStateChangeEvent{
		UpperLevelSSTableIds: []uint64{1, 2, 3, 4},
	}
	excludedSSTableIds := event.allSSTableIdsExcludingTheOnesPresentInUpperLevelSSTableIds([]uint64{1, 2, 3, 4, 5, 6})

	assert.Equal(t, []uint64{5, 6}, excludedSSTableIds)
}
