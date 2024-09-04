package log

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWALDirectoryPath(t *testing.T) {
	walPath := NewWALPath(".")
	defer func() {
		_ = os.RemoveAll(walPath.DirectoryPath)
	}()

	assert.Equal(t, "wal", walPath.DirectoryPath)
}

func TestWALDirectoryPathWithANewDirectoryCreatedForWAL(t *testing.T) {
	walPath := NewWALPath(".")
	defer func() {
		_ = os.RemoveAll(walPath.DirectoryPath)
	}()

	_, err := os.Stat(walPath.DirectoryPath)
	assert.Nil(t, err)
}
