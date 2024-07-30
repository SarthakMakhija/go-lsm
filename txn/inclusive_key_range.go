package txn

type InclusiveKeyRange struct {
	start Key
	end   Key
}

func NewInclusiveKeyRange(start, end Key) InclusiveKeyRange {
	if end.CompareKeysWithDescendingTimestamp(start) < 0 {
		panic("end must be greater than start")
	}
	return InclusiveKeyRange{
		start: start,
		end:   end,
	}
}

func (inclusiveKeyRange InclusiveKeyRange) Start() Key {
	return inclusiveKeyRange.start
}

func (inclusiveKeyRange InclusiveKeyRange) End() Key {
	return inclusiveKeyRange.end
}
