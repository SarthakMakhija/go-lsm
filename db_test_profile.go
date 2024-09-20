//go:build test

package go_lsm

import "go-lsm/state"

// StorageState returns the StorageState, it is only for testing.
func (db *Db) StorageState() *state.StorageState {
	return db.storageState
}
