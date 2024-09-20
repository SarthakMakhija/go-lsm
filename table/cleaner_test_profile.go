//go:build test

package table

// PendingSSTablesToClean returns all the SSTables that are pending to be cleaned, it is only for testing.
func (cleaner *SSTableCleaner) PendingSSTablesToClean() []*SSTable {
	return cleaner.pending
}
