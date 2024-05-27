package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidInclusiveRange(t *testing.T) {
	assert.Panics(t, func() {
		NewInclusiveKeyRange(NewStringKey("consensus"), NewStringKey("accurate"))
	})
}

func TestInclusiveRang(t *testing.T) {
	inclusiveRange := NewInclusiveKeyRange(NewStringKey("consensus"), NewStringKey("distributed"))
	assert.Equal(t, NewStringKey("consensus"), inclusiveRange.Start())
	assert.Equal(t, NewStringKey("distributed"), inclusiveRange.End())
}
