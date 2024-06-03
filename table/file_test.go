package table

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestReadFixedChunkFromFile(t *testing.T) {
	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestReadFixedChunkFromFile.log")

	value := []byte("LSM Tree: Log storage merge tree")
	file, err := Create(filePath, value)

	assert.Nil(t, err)

	buffer := make([]byte, len(value))
	n, err := file.Read(0, buffer)

	assert.Nil(t, err)
	assert.Equal(t, value, buffer[:n])
}

func TestReadMoreChunkThanAvailableFromFile(t *testing.T) {
	tempDirectory := os.TempDir()
	filePath := filepath.Join(tempDirectory, "TestReadMoreChunkThanAvailableFromFile.log")

	value := []byte("LSM Tree: Log storage merge tree")
	file, err := Create(filePath, value)

	assert.Nil(t, err)

	buffer := make([]byte, 2*1024)
	n, err := file.Read(0, buffer)

	assert.Nil(t, err)
	assert.Equal(t, value, buffer[:n])
}
