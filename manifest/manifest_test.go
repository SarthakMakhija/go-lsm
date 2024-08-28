package manifest

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateANewManifestWithNewMemtableCreatedEvent(t *testing.T) {
	manifestDirectoryPath := filepath.Join(os.TempDir(), "TestCreateANewManifestWithNewMemtableCreatedEvent")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		manifest.Stop()
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	future := manifest.Submit(NewMemtableCreated(10))
	future.Wait()
}

func TestCreateANewManifestWithNewSSTableFlushedEvent(t *testing.T) {
	manifestDirectoryPath := filepath.Join(os.TempDir(), "TestCreateANewManifestWithNewSSTableFlushedEvent")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		manifest.Stop()
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	future := manifest.Submit(NewSSTableFlushed(10))
	future.Wait()
}
