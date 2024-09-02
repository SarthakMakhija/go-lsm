package manifest

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Manifest records different events in the system.
// The events are described by the Event interface.
type Manifest struct {
	file        *os.File
	writeLock   sync.RWMutex
	stopChannel chan struct{}
}

// CreateNewOrRecoverFrom either creates a new Manifest or recovers from an existing manifest file.
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
		file:        file,
		stopChannel: make(chan struct{}),
	}
	events, err := manifest.attemptRecovery()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return manifest, events, nil
}

// Add adds the event to the manifest file.
func (manifest *Manifest) Add(event Event) error {
	manifest.writeLock.Lock()
	defer manifest.writeLock.Unlock()

	buf, err := event.encode()
	if err != nil {
		slog.Warn("error while serializing event: %s", err)
		return err
	}
	if _, err = manifest.file.Write(buf); err != nil {
		slog.Warn("error while writing event: %s", err)
		return err
	}
	return manifest.file.Sync()
}

// attemptRecovery attempts recovery of events from the Manifest file.
// This implementation reads the whole file and passes the byte slice to decodeEventsFrom() method.
// This implementation does not perform truncation (or compaction) of Manifest file, which means if the system runs for some time,
// the size of the file will increase.
func (manifest *Manifest) attemptRecovery() ([]Event, error) {
	bytes, err := io.ReadAll(manifest.file)
	if err != nil {
		return nil, err
	}
	return decodeEventsFrom(bytes), nil
}
