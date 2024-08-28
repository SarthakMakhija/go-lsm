package manifest

import (
	"go-lsm/future"
	"log/slog"
	"os"
	"path/filepath"
)

const recordChannelSize = 8 * 1024

// EventContext represents an event with a future. The future id marked done when the event is applied to manifest file.
type EventContext struct {
	event  Event
	future *future.Future
}

// Manifest records different events in the system.
// Different events are described by Event interface.
type Manifest struct {
	file         *os.File
	stopChannel  chan struct{}
	eventChannel chan EventContext
}

// CreateNewOrRecoverFrom either creates a new Manifest or recovers from an existing manifest file.
// Also, starts a single goroutine that writes to manifest file.
func CreateNewOrRecoverFrom(directoryPath string) (*Manifest, error) {
	path := filepath.Join(directoryPath, "manifest")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		_, err := os.Create(path)
		if err != nil {
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest{
		file:         file,
		stopChannel:  make(chan struct{}),
		eventChannel: make(chan EventContext, recordChannelSize),
	}
	manifest.spin()
	return manifest, nil
	//TODO: Recovery
}

// Submit submits an event to the event channel.
func (manifest *Manifest) Submit(event Event) *future.Future {
	newFuture := future.NewFuture()
	manifest.eventChannel <- EventContext{event: event, future: newFuture}

	return newFuture
}

// Stop stops the manifest.
func (manifest *Manifest) Stop() {
	close(manifest.stopChannel)
}

// spin runs a goroutine that receives events from eventChannel and applies them serially in the manifest file.
// It is an implementation of [singular update queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html).
// This also means that events are eventually applied to Manifest.
func (manifest *Manifest) spin() {
	go func() {
		for {
			select {
			case <-manifest.stopChannel:
				_ = manifest.file.Close()
				return
			case eventContext := <-manifest.eventChannel:
				buf, err := eventContext.event.serialize()
				if err != nil {
					slog.Warn("error while serializing event: %s", err)
					continue
				}
				if _, err = manifest.file.Write(buf); err != nil {
					slog.Warn("error while writing event: %s", err)
				}
				eventContext.future.MarkDone()
			}
		}
	}()
}
