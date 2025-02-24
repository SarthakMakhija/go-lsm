package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWALDirectoryPath(t *testing.T) {
	walPath := NewWALPath(".")
	defer func() {
		_ = os.RemoveAll(walPath.DirectoryPath)
	}()

	assert.Equal(t, "wal", walPath.DirectoryPath)
}

func TestWALDirectoryPathAssertANewDirectoryIsCreatedForWAL(t *testing.T) {
	walPath := NewWALPath(".")
	defer func() {
		_ = os.RemoveAll(walPath.DirectoryPath)
	}()

	_, err := os.Stat(walPath.DirectoryPath)
	assert.Nil(t, err)
}
