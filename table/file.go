package table

import (
	"io"
	"os"
)

// File represents SSTable file.
type File struct {
	file *os.File
	size int64
}

// CreateAndWrite creates a new SSTable file and writes the given data.
// It opens the file in readonly mode and returns the file handle.
func CreateAndWrite(path string, data []byte) (*File, error) {
	err := syncWrite(path, data)
	if err != nil {
		return nil, err
	}
	return openReadonly(path, data)
}

// Open opens the file at the filePath in readonly mode.
// Is used when loading the state.StorageState.
func Open(filePath string) (*File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &File{
		file: file,
		size: stat.Size(),
	}, nil
}

// Read reads the file of buffer size from the given offset.
// It requires a seek to the given offset from the file start, and then a read operation into the buffer.
func (file *File) Read(offset int64, buffer []byte) (int, error) {
	if _, err := file.file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := file.file.Read(buffer)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Size returns the file size.
func (file *File) Size() int64 {
	return file.size
}

// syncWrite performs fsync operation after writing the data to the file.
// The file is closed after syncWrite.
func syncWrite(path string, data []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	n, err := file.Write(data)
	if err != nil {
		return err
	}
	if n < len(data) {
		return io.ErrShortWrite
	}
	_ = file.Sync()
	return nil
}

// openReadonly opens the file in readonly mode.
func openReadonly(path string, data []byte) (*File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &File{
		file: file,
		size: int64(len(data)),
	}, nil
}
