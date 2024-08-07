package txn

type LessOrEqual interface {
	IsLessThanOrEqualTo(other LessOrEqual) bool
}

type InclusiveKeyRange[T LessOrEqual] struct {
	start T
	end   T
}

func NewInclusiveKeyRange[T LessOrEqual](start, end T) InclusiveKeyRange[T] {
	if start.IsLessThanOrEqualTo(end) {
		return InclusiveKeyRange[T]{
			start: start,
			end:   end,
		}
	}
	panic("end key must be greater than or equal to start key in InclusiveKeyRange")
}

func (inclusiveKeyRange InclusiveKeyRange[T]) Start() T {
	return inclusiveKeyRange.start
}

func (inclusiveKeyRange InclusiveKeyRange[T]) End() T {
	return inclusiveKeyRange.end
}
