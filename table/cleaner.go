package table

import (
	"log/slog"
	"time"
)

const inboundChannelCapacity = 1024

// SSTableCleaner cleans SSTable(s) after compaction.
// Consider that SSTables S1 and S2 undergo compaction and result in creation of new SSTable S3.
// After compaction, S1 and S2 need to be removed (/cleaned/deleted).
// However, it is possible that some running transactions might have created iterators over S1 and S2.
// Thus, the system can not delete these files (/SSTables).
// Hence, SSTableCleaner cleans SSTables if their reference count reaches zero.
type SSTableCleaner struct {
	inboundChannel                    chan []*SSTable
	stopChannel                       chan struct{}
	stopCompletionNotificationChannel chan struct{}
	pending                           []*SSTable
	cleanDuration                     time.Duration
}

// NewSSTableCleaner creates a new instance of SSTableCleaner.
func NewSSTableCleaner(cleanDuration time.Duration) *SSTableCleaner {
	return &SSTableCleaner{
		inboundChannel:                    make(chan []*SSTable, inboundChannelCapacity),
		stopChannel:                       make(chan struct{}),
		stopCompletionNotificationChannel: make(chan struct{}),
		cleanDuration:                     cleanDuration,
	}
}

// Start starts the SSTableCleaner.
// As a part of cleaner operation, it does the following:
// 1) Attempt to clean SSTable(s) received from inboundChannel.
// 2) Attempt to clean all the pending SSTable(s) at fixed interval.
// The goroutine is co-operative, and it honors the notification on stopChannel.
func (cleaner *SSTableCleaner) Start() {
	go func() {
		pendingCleanTimer := time.NewTimer(cleaner.cleanDuration)
		defer pendingCleanTimer.Stop()

		for {
			select {
			case ssTables := <-cleaner.inboundChannel:
				cleaner.mayBeClean(ssTables)
			case <-pendingCleanTimer.C:
				cleaner.mayBeCleanPending()
				pendingCleanTimer.Reset(cleaner.cleanDuration)
			case <-cleaner.stopChannel:
				close(cleaner.stopCompletionNotificationChannel)
				return
			}
		}
	}()
}

// Submit submits SSTables to the SSTableCleaner for deletion.
func (cleaner *SSTableCleaner) Submit(ssTables []*SSTable) {
	cleaner.inboundChannel <- ssTables
}

// Stop stops the SSTableCleaner.
func (cleaner *SSTableCleaner) Stop() chan struct{} {
	close(cleaner.stopChannel)
	return cleaner.stopCompletionNotificationChannel
}

// mayBeClean cleans (/removes) SSTables if their reference count reaches zero.
func (cleaner *SSTableCleaner) mayBeClean(ssTables []*SSTable) {
	for _, ssTable := range ssTables {
		if !cleaner.mayBeCleanAnSSTable(ssTable) {
			cleaner.pending = append(cleaner.pending, ssTable)
		}
	}
}

// mayBeCleanPending cleans the pending SSTables if their reference count has reached zero.
// It also updates the state of SSTableCleaner.
func (cleaner *SSTableCleaner) mayBeCleanPending() {
	var unableToClean []*SSTable
	for _, ssTable := range cleaner.pending {
		if !cleaner.mayBeCleanAnSSTable(ssTable) {
			unableToClean = append(unableToClean, ssTable)
		}
	}
	cleaner.pending = nil
	cleaner.pending = unableToClean
}

// mayBeCleanAnSSTable cleans the SSTable if their reference count reaches zero.
func (cleaner *SSTableCleaner) mayBeCleanAnSSTable(ssTable *SSTable) bool {
	if ssTable.TotalReferences() <= 0 {
		if err := ssTable.Remove(); err != nil {
			slog.Error("error in removing ssTable", err)
			return false
		}
		return true
	}
	return false
}
