package log

import (
	"os"
	"path/filepath"
)

// WALPath is a wrapper over the directory path of WAL.
type WALPath struct {
	DirectoryPath string
}

// NewWALPath creates a new instance of WALPath.
func NewWALPath(rootPath string) WALPath {
	walDirectoryPath := filepath.Join(rootPath, "wal")
	if _, err := os.Stat(walDirectoryPath); os.IsNotExist(err) {
		_ = os.MkdirAll(walDirectoryPath, os.ModePerm)
	}
	return WALPath{
		DirectoryPath: walDirectoryPath,
	}
}
