package future

// Future represents the result of asynchronous computation.
// Eg; a response to committing a batch. It allows the clients to wait until the batch is applied to
// the state machine. Please check txn.Executor.
// Another example is applying an event to manifest.Manifest.
type Future struct {
	responseChannel chan struct{}
	isDone          bool
}

// NewFuture creates a new instance of Future.
func NewFuture() *Future {
	return &Future{
		responseChannel: make(chan struct{}),
		isDone:          false,
	}
}

// MarkDone marks the Future as done.
func (future *Future) MarkDone() {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
}

// Wait waits until the Future is marked as done.
func (future *Future) Wait() {
	<-future.responseChannel
}
