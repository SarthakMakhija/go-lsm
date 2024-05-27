package table

import (
	"cmp"
)

type InclusiveRange[T cmp.Ordered] struct {
	start, end T
}

func NewInclusiveRange[T cmp.Ordered](start, end T) InclusiveRange[T] {
	if end < start {
		panic("end must be greater than start")
	}
	return InclusiveRange[T]{
		start: start,
		end:   end,
	}
}

func (inclusiveRange InclusiveRange[T]) Start() T {
	return inclusiveRange.start
}

func (inclusiveRange InclusiveRange[T]) End() T {
	return inclusiveRange.end
}
