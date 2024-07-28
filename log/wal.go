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
	_, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

func Recover(path string, callback func(key txn.Key, value txn.Value)) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	for len(bytes) > 0 {
		keySize := binary.LittleEndian.Uint16(bytes)
		key := bytes[block.ReservedKeySize : uint16(block.ReservedKeySize)+keySize]

		valueSize := binary.LittleEndian.Uint16(bytes[uint16(block.ReservedKeySize)+keySize:])
		value := bytes[uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize) : uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize)+valueSize]

		callback(txn.NewKey(key), txn.NewValue(value))
		bytes = bytes[uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize)+valueSize:]
	}
	return &WAL{file: file}, nil
}

func (wal *WAL) Append(key txn.Key, value txn.Value) error {
	buffer := make([]byte, key.SizeInBytes()+value.SizeInBytes()+block.ReservedKeySize+block.ReservedValueSize)

	binary.LittleEndian.PutUint16(buffer, uint16(key.SizeInBytes()))
	copy(buffer[block.ReservedKeySize:], key.Bytes())

	binary.LittleEndian.PutUint16(buffer[block.ReservedKeySize+key.SizeInBytes():], uint16(value.SizeInBytes()))
	copy(buffer[block.ReservedKeySize+key.SizeInBytes()+block.ReservedValueSize:], value.Bytes())

	_, err := wal.file.Write(buffer)
	return err
}

func (wal *WAL) Sync() error {
	return wal.file.Sync()
}

func (wal WAL) Close() {
	_ = wal.file.Close()
}
