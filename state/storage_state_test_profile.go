//go:build test

package state

import (
	"go-lsm/manifest"
	"go-lsm/memory"
)

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
	storageState.manifest.Submit(manifest.NewMemtableCreated(storageState.currentMemtable.Id()))
}
