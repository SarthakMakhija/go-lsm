//go:build test

package test_utility

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func SetupADirectoryWithTestName(t *testing.T) string {
	directory := "."
	rootPath := filepath.Join(directory, t.Name())

	assert.Nil(t, os.MkdirAll(rootPath, os.ModePerm))
	return rootPath
}

func CleanupDirectoryWithTestName(t *testing.T) {
	directory := "."
	rootPath := filepath.Join(directory, t.Name())

	assert.Nil(t, os.RemoveAll(rootPath))
}
