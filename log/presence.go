package log

import (
	"os"
	"path/filepath"
)

// WalPresence indicates the presence of WAL.
type WalPresence struct {
	EnableWAL        bool
	WALDirectoryPath string
}

// NewWALPresence creates a new instance of WalPresence.
func NewWALPresence(enableWAL bool, directoryPath string) *WalPresence {
	var walDirectoryPath = ""
	if enableWAL {
		walDirectoryPath = filepath.Join(directoryPath, "wal")
		if _, err := os.Stat(walDirectoryPath); os.IsNotExist(err) {
			_ = os.MkdirAll(walDirectoryPath, os.ModePerm)
		}
	}
	return &WalPresence{
		EnableWAL:        enableWAL,
		WALDirectoryPath: walDirectoryPath,
	}
}
