package table

import (
	"log/slog"
	"time"
)

const inboundChannelCapacity = 1024

type SSTableCleaner struct {
	inboundChannel chan []*SSTable
	stopChannel    chan struct{}
	pending        []*SSTable
	cleanDuration  time.Duration
}

func NewSSTableCleaner(cleanDuration time.Duration) *SSTableCleaner {
	return &SSTableCleaner{
		inboundChannel: make(chan []*SSTable, inboundChannelCapacity),
		stopChannel:    make(chan struct{}),
		cleanDuration:  cleanDuration,
	}
}

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
				return
			}
		}
	}()
}

func (cleaner *SSTableCleaner) Submit(ssTables []*SSTable) {
	cleaner.inboundChannel <- ssTables
}

func (cleaner *SSTableCleaner) Stop() {
	close(cleaner.stopChannel)
}

func (cleaner *SSTableCleaner) mayBeClean(ssTables []*SSTable) {
	for _, ssTable := range ssTables {
		if !cleaner.mayBeCleanAnSSTable(ssTable) {
			cleaner.pending = append(cleaner.pending, ssTable)
		}
	}
}

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
