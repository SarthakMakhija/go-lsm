package future

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFutureWithOkStatus(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	future := NewFuture()
	go func() {
		defer wg.Done()
		future.Wait()

		assert.True(t, future.isDone)
		assert.True(t, future.Status().IsOk())
	}()

	future.MarkDoneAsOk()
	wg.Wait()
}

func TestFutureWithErrorStatus(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	future := NewFuture()
	go func() {
		defer wg.Done()
		future.Wait()

		assert.True(t, future.isDone)
		assert.True(t, future.Status().IsErr())
	}()

	future.MarkDoneAsError(errors.New("test error"))
	wg.Wait()
}
