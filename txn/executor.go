package txn

import (
	"go-lsm"
	"go-lsm/kv"
	"sync"
)

const incomingChannelSize = 1 * 1024

type Executor struct {
	state           *go_lsm.StorageState
	incomingChannel chan ExecutionRequest
	stopChannel     chan struct{}
	stopOnce        sync.Once
}

func NewExecutor(state *go_lsm.StorageState) *Executor {
	executor := &Executor{
		state:           state,
		incomingChannel: make(chan ExecutionRequest, incomingChannelSize),
		stopChannel:     make(chan struct{}),
	}
	go executor.start()
	return executor
}

func (executor *Executor) start() {
	for {
		select {
		case executionRequest := <-executor.incomingChannel:
			executor.state.Set(executionRequest.batch)
			executionRequest.future.markDone()
		case <-executor.stopChannel:
			close(executor.incomingChannel)
			return
		}
	}
}

func (executor *Executor) submit(batch kv.TimestampedBatch) *Future {
	executionRequest := NewExecutionRequest(batch)
	executor.incomingChannel <- executionRequest
	return executionRequest.future
}

func (executor *Executor) stop() {
	executor.stopOnce.Do(func() {
		close(executor.stopChannel)
	})
}

//////// ExecutionRequest & Future ////////////////

type ExecutionRequest struct {
	batch  kv.TimestampedBatch
	future *Future
}

type Future struct {
	responseChannel chan struct{}
	isDone          bool
}

func NewExecutionRequest(batch kv.TimestampedBatch) ExecutionRequest {
	return ExecutionRequest{
		batch:  batch,
		future: NewFuture(),
	}
}

func NewFuture() *Future {
	return &Future{
		responseChannel: make(chan struct{}),
		isDone:          false,
	}
}

func (future *Future) markDone() {
	if !future.isDone {
		close(future.responseChannel)
		future.isDone = true
	}
}

func (future *Future) Wait() {
	<-future.responseChannel
}
