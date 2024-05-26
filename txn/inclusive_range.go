package txn

type InclusiveRange struct {
	start Key
	end   Key
}

func NewInclusiveRange(start, end Key) InclusiveRange {
	if end.Compare(start) < 0 {
		panic("end must be greater than start")
	}
	return InclusiveRange{
		start: start,
		end:   end,
	}
}

func (inclusiveRange InclusiveRange) Start() Key {
	return inclusiveRange.start
}

func (inclusiveRange InclusiveRange) End() Key {
	return inclusiveRange.end
}
