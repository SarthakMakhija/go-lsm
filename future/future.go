package future

// Future represents the result of asynchronous computation.
// Eg; a response to committing a batch. It allows the clients to wait until the batch is applied to
// the state machine. Please check txn.Executor.
// Another example is applying an event to manifest.Manifest.
type Future struct {
	responseChannel chan struct{}
	isDone          bool
	status          Status
}

// NewFuture creates a new instance of Future.
func NewFuture() *Future {
	return &Future{
		responseChannel: make(chan struct{}),
		isDone:          false,
	}
}

// MarkDoneAsOk marks the Future as done with Status Ok.
func (future *Future) MarkDoneAsOk() {
	future.markDone()
	future.status = OkStatus()
}

// MarkDoneAsError marks the Future as done with Status Error.
func (future *Future) MarkDoneAsError(err error) {
	future.markDone()
	future.status = ErrorStatus(err)
}

// Wait waits until the Future is marked as done.
func (future *Future) Wait() {
	<-future.responseChannel
}

// Status returns the status.
func (future *Future) Status() Status {
	return future.status
}

// markDone marks the future as done and closes the responseChannel.
func (future *Future) markDone() {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
}
