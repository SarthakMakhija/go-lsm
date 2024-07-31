package txn

type InclusiveKeyRange struct {
	start Key
	end   Key
}

func NewInclusiveKeyRange(start, end Key) InclusiveKeyRange {
	if start.IsLessThanOrEqualTo(end) {
		return InclusiveKeyRange{
			start: start,
			end:   end,
		}
	}
	panic("end key must be greater than or equal to start key in InclusiveKeyRange")
}

func (inclusiveKeyRange InclusiveKeyRange) Start() Key {
	return inclusiveKeyRange.start
}

func (inclusiveKeyRange InclusiveKeyRange) End() Key {
	return inclusiveKeyRange.end
}
