//go:build test

package txn

import "context"

// SetBeginTimestamp sets the begin-timestamp, only for testing.
func (oracle *Oracle) SetBeginTimestamp(timestamp uint64) {
	oracle.beginTimestampMark.Finish(timestamp)
	if err := oracle.beginTimestampMark.WaitForMark(context.Background(), timestamp); err != nil {
		panic(err)
	}
}
