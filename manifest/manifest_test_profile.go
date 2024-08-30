//go:build test

package manifest

import "os"

// Delete deletes Manifest file, only for testing.
func (manifest *Manifest) Delete() {
	_ = manifest.file.Close()
	_ = os.Remove(manifest.file.Name())
}
