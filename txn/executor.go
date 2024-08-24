package txn

import (
	"go-lsm/kv"
	"go-lsm/state"
	"sync"
)

const incomingChannelSize = 1 * 1024

// Executor is an implementation of [Singular Update Queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html).
// Executor applies all the commits sequentially.
//
// It is a single goroutine that reads kv.TimestampedBatch from the incomingChannel.
// Anytime a Readwrite Transaction is ready to commit, its kv.TimestampedBatch is sent to the TransactionExecutor via the Submit() method.
// Executor applies the batch to the instance of state.StorageState.
type Executor struct {
	state           *state.StorageState
	incomingChannel chan ExecutionRequest
	stopChannel     chan struct{}
	stopOnce        sync.Once
}

// NewExecutor creates a new instance of Executor, and starts a single goroutine which will apply the commits sequentially.
// It is called once in the entire application.
func NewExecutor(state *state.StorageState) *Executor {
	executor := &Executor{
		state:           state,
		incomingChannel: make(chan ExecutionRequest, incomingChannelSize),
		stopChannel:     make(chan struct{}),
	}
	go executor.start()
	return executor
}

// start starts the executor.
// Everytime the executor receives an instance of kv.TimestampedBatch from incomingChannel, it applies it to the state.StorageState,
// calls the callback present in the executionRequest, and mark the corresponding future as done.
func (executor *Executor) start() {
	for {
		select {
		case executionRequest := <-executor.incomingChannel:
			executor.state.Set(executionRequest.batch)
			executionRequest.callback()
			executionRequest.future.markDone()
		case <-executor.stopChannel:
			close(executor.incomingChannel)
			return
		}
	}
}

// submit submits the kv.TimestampedBatch along with callback to the Executor.
// kv.TimestampedBatch and callback is wrapped in ExecutionRequest.
// It returns an instance of Future to allow the clients to wait until the transactional batch is applied to the state machine.
func (executor *Executor) submit(batch kv.TimestampedBatch, callback func()) *Future {
	executionRequest := NewExecutionRequest(batch, callback)
	executor.incomingChannel <- executionRequest
	return executionRequest.future
}

// stop stops the Executor.
func (executor *Executor) stop() {
	executor.stopOnce.Do(func() {
		close(executor.stopChannel)
	})
}

//////// ExecutionRequest & Future ////////////////

// ExecutionRequest wraps the kv.TimestampedBatch along with a callback.
type ExecutionRequest struct {
	batch    kv.TimestampedBatch
	callback func()
	future   *Future
}

// Future is a response to the submission of kv.TimestampedBatch. It allows the clients to wait until the batch is applied to
// the state machine.
type Future struct {
	responseChannel chan struct{}
	isDone          bool
}

// NewExecutionRequest creates a new instance of ExecutionRequest.
func NewExecutionRequest(batch kv.TimestampedBatch, callback func()) ExecutionRequest {
	return ExecutionRequest{
		batch:    batch,
		callback: callback,
		future:   NewFuture(),
	}
}

// NewFuture creates a new instance of Future.
func NewFuture() *Future {
	return &Future{
		responseChannel: make(chan struct{}),
		isDone:          false,
	}
}

// markDone marks the Future as done.
func (future *Future) markDone() {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
}

// Wait waits until the Future is maked as done.
func (future *Future) Wait() {
	<-future.responseChannel
}
