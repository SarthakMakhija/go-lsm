package kv

// LessOrEqual defines a contract for any key to be a part of InclusiveKeyRange.
// Currently, Key and RawKey implement LessOrEqual.
type LessOrEqual interface {
	IsLessThanOrEqualTo(other LessOrEqual) bool
}

// InclusiveKeyRange represents a key range with an inclusive end key.
// To keep things simple, the implementation only supports inclusive range in scan or scan-like operations.
type InclusiveKeyRange[T LessOrEqual] struct {
	start T
	end   T
}

// NewInclusiveKeyRange creates a new instance of InclusiveKeyRange if start is less than or equal to the end, panics otherwise.
func NewInclusiveKeyRange[T LessOrEqual](start, end T) InclusiveKeyRange[T] {
	if start.IsLessThanOrEqualTo(end) {
		return InclusiveKeyRange[T]{
			start: start,
			end:   end,
		}
	}
	panic("end key must be greater than or equal to start key in InclusiveKeyRange")
}

// Start returns the start key.
func (inclusiveKeyRange InclusiveKeyRange[T]) Start() T {
	return inclusiveKeyRange.start
}

// End returns the end key.
func (inclusiveKeyRange InclusiveKeyRange[T]) End() T {
	return inclusiveKeyRange.end
}
