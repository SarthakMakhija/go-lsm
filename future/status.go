package future

type StatusType int

const (
	Ok    StatusType = 1
	Error StatusType = 2
)

// Status represents the status of an async operation that returns a Future.
type Status struct {
	StatusType
	Err error
}

// OkStatus creates a new Status with StatusType as Ok.
func OkStatus() Status {
	return Status{StatusType: Ok, Err: nil}
}

// ErrorStatus creates a new Status with StatusType as Error.
func ErrorStatus(err error) Status {
	return Status{StatusType: Error, Err: err}
}

// IsOk returns true if the StatusType is Ok.
func (status Status) IsOk() bool {
	return status.StatusType == Ok
}

// IsOk returns true if the StatusType is Error.
func (status Status) IsErr() bool {
	return status.StatusType == Error
}
