//go:build test

package state

import (
	"go-lsm/manifest"
	"go-lsm/memory"
	"time"
)

func NewStorageState() (*StorageState, error) {
	return NewStorageStateWithOptions(StorageOptions{
		MemTableSizeInBytes:   1 << 20,
		Path:                  ".",
		MaximumMemtables:      5,
		FlushMemtableDuration: 50 * time.Millisecond,
		EnableWAL:             false,
		compactionOptions: SimpleLeveledCompactionOptions{
			level0FilesCompactionTrigger: 6,
			maxLevels:                    totalLevels,
			sizeRatioPercentage:          200,
		},
	})
}

// DeleteManifest deletes Manifest file, only for testing.
func (storageState *StorageState) DeleteManifest() {
	storageState.manifest.Delete()
}

// forceFreezeCurrentMemtable freezes the current memtable, it is only for testing.
func (storageState *StorageState) forceFreezeCurrentMemtable() {
	storageState.immutableMemtables = append(storageState.immutableMemtables, storageState.currentMemtable)
	storageState.currentMemtable = memory.NewMemtable(
		storageState.idGenerator.NextId(),
		storageState.options.MemTableSizeInBytes,
		memory.NewWALPresence(storageState.options.EnableWAL, storageState.walDirectoryPath),
	)
	_ = storageState.manifest.Add(manifest.NewMemtableCreated(storageState.currentMemtable.Id()))
}
