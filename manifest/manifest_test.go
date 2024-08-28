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

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		manifest.Stop()
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	future := manifest.Submit(NewSSTableFlushed(10))
	future.Wait()
}

func TestRecoversAnExistingManifest(t *testing.T) {
	manifestDirectoryPath := filepath.Join(os.TempDir(), "TestRecoversAnExistingManifest")
	assert.Nil(t, os.MkdirAll(manifestDirectoryPath, os.ModePerm))

	manifest, _, err := CreateNewOrRecoverFrom(manifestDirectoryPath)

	defer func() {
		_ = os.RemoveAll(manifestDirectoryPath)
	}()

	assert.Nil(t, err)
	future := manifest.Submit(NewMemtableCreated(10))
	future.Wait()

	future = manifest.Submit(NewMemtableCreated(20))
	future.Wait()

	future = manifest.Submit(NewSSTableFlushed(10))
	future.Wait()

	manifest.Stop()

	manifest, events, err := CreateNewOrRecoverFrom(manifestDirectoryPath)
	assert.Nil(t, err)

	manifest.Stop()

	assert.Equal(t, 3, len(events))
	assert.Equal(t, uint64(10), events[0].(*MemtableCreated).memtableId)
	assert.Equal(t, uint64(20), events[1].(*MemtableCreated).memtableId)
	assert.Equal(t, uint64(10), events[2].(*SSTableFlushed).ssTableId)
}
