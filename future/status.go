package future

type StatusType int

const (
	Ok    StatusType = 1
	Error StatusType = 2
)

type Status struct {
	StatusType
	Err error
}

func OkStatus() Status {
	return Status{StatusType: Ok, Err: nil}
}

func ErrorStatus(err error) Status {
	return Status{StatusType: Error, Err: err}
}

func (status Status) IsOk() bool {
	return status.StatusType == Ok
}
