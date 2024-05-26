package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidInclusiveRange(t *testing.T) {
	assert.Panics(t, func() {
		NewInclusiveRange(NewStringKey("consensus"), NewStringKey("accurate"))
	})
}

func TestInclusiveRang(t *testing.T) {
	inclusiveRange := NewInclusiveRange(NewStringKey("consensus"), NewStringKey("distributed"))
	assert.Equal(t, NewStringKey("consensus"), inclusiveRange.Start())
	assert.Equal(t, NewStringKey("distributed"), inclusiveRange.End())
}
