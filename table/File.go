package table

import (
	"io"
	"os"
)

type File struct {
	file *os.File
	size int64
}

func Create(path string, data []byte) (*File, error) {
	err := syncWrite(path, data)
	if err != nil {
		return nil, err
	}
	return openReadonly(path, data)
}

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

func (file *File) Size() int64 {
	return file.size
}

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
