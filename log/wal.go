package log

import (
	"encoding/binary"
	"fmt"
	"go-lsm/table/block"
	"go-lsm/txn"
	"io"
	"os"
	"path/filepath"
)

type WAL struct {
	file *os.File
}

func NewWALForId(id uint64, walDirectoryPath string) (*WAL, error) {
	return NewWAL(filepath.Join(walDirectoryPath, fmt.Sprintf("%v.wal", id)))
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func (wal *WAL) Append(key txn.Key, value txn.Value) error {
	buffer := make([]byte, key.Size()+value.Size()+block.ReservedKeySize+block.ReservedValueSize)

	binary.LittleEndian.PutUint16(buffer, uint16(key.Size()))
	copy(buffer[block.ReservedKeySize:], key.Bytes())

	binary.LittleEndian.PutUint16(buffer[block.ReservedKeySize+key.Size():], uint16(value.Size()))
	copy(buffer[block.ReservedKeySize+key.Size()+block.ReservedValueSize:], value.Bytes())

	_, err := wal.file.Write(buffer)
	return err
}

func Recover(path string, callback func(key txn.Key, value txn.Value)) error {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	for len(bytes) > 0 {
		keySize := binary.LittleEndian.Uint16(bytes)
		key := bytes[block.ReservedKeySize : uint16(block.ReservedKeySize)+keySize]

		valueSize := binary.LittleEndian.Uint16(bytes[uint16(block.ReservedKeySize)+keySize:])
		value := bytes[uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize) : uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize)+valueSize]

		callback(txn.NewKey(key), txn.NewValue(value))
		bytes = bytes[uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize)+valueSize:]
	}
	return nil
}

func (wal *WAL) Sync() error {
	return wal.file.Sync()
}

func (wal WAL) Close() {
	_ = wal.file.Close()
}
