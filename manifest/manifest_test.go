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

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
}

func TestCreateANewManifestWithNewSSTableFlushedEvent(t *testing.T) {
	manifestDirectoryPath := filepath.Join(os.TempDir(), "TestCreateANewManifestWithNewSSTableFlushedEvent")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewSSTableFlushed(10)))
}

func TestRecoversAnExistingManifest(t *testing.T) {
	manifestDirectoryPath := filepath.Join(os.TempDir(), "TestRecoversAnExistingManifest")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	assert.Nil(t, manifest.Add(NewMemtableCreated(10)))
	assert.Nil(t, manifest.Add(NewMemtableCreated(20)))
	assert.Nil(t, manifest.Add(NewSSTableFlushed(10)))

	manifest, events, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	assert.Nil(t, err)

	assert.Equal(t, 3, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).memtableId)
	assert.Equal(t, uint64(20), events[1].(*MemtableCreated).memtableId)
	assert.Equal(t, uint64(10), events[2].(*SSTableFlushed).ssTableId)
}
