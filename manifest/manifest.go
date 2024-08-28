package manifest

import (
	"go-lsm/future"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

const recordChannelSize = 8 * 1024

// EventContext represents an event with a future.
// The future is marked done when the event is applied to manifest file.
type EventContext struct {
	event  Event
	future *future.Future
}

// Manifest records different events in the system.
// The events are described by the Event interface.
type Manifest struct {
	file         *os.File
	stopChannel  chan struct{}
	eventChannel chan EventContext
}

// CreateNewOrRecoverFrom either creates a new Manifest or recovers from an existing manifest file.
// Also, starts a single goroutine that writes events to the manifest file.
func CreateNewOrRecoverFrom(directoryPath string) (*Manifest, []Event, error) {
	path := filepath.Join(directoryPath, "manifest")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		_, err := os.Create(path)
		if err != nil {
			return nil, nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, err
	}
	manifest := &Manifest{
		file:         file,
		stopChannel:  make(chan struct{}),
		eventChannel: make(chan EventContext, recordChannelSize),
	}
	events, err := manifest.attemptRecovery()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	manifest.spin()
	return manifest, events, nil
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
// This implementation means that events are eventually applied to Manifest.
// There is also a possibility of event loss with this design.
// Consider the following example:
// A new memtable was created and at the same time a memtable was flushed to SSTable.
// This would result in submission of two events to Manifest.
// However, the events would be applied eventually. It is possible that the key/value database
// shuts down before both the events are applied.
func (manifest *Manifest) spin() {
	go func() {
		for {
			select {
			case <-manifest.stopChannel:
				_ = manifest.file.Close()
				return
			case eventContext := <-manifest.eventChannel:
				buf, err := eventContext.event.encode()
				if err != nil {
					slog.Warn("error while serializing event: %s", err)
					continue
				}
				if _, err = manifest.file.Write(buf); err != nil {
					slog.Warn("error while writing event: %s", err)
				}
				if err = manifest.file.Sync(); err != nil {
					slog.Warn("error while performing fsync on the manifest file: %s", err)
				}
				eventContext.future.MarkDone()
			}
		}
	}()
}

// attemptRecovery attempts recovery of events from the Manifest file.
// This implementation reads the whole file and passes the byte slice to decodeEventsFrom() method.
// This implementation does not perform truncation (or compaction) of Manifest file, which means if the system runs for sometime,
// the size of the file will increase.
func (manifest *Manifest) attemptRecovery() ([]Event, error) {
	bytes, err := io.ReadAll(manifest.file)
	if err != nil {
		return nil, err
	}
	return decodeEventsFrom(bytes), nil
}
