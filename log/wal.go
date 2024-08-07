package log

import (
	"encoding/binary"
	"fmt"
	"go-lsm/kv"
	"go-lsm/table/block"
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

func Recover(path string, callback func(key kv.Key, value kv.Value)) (*WAL, error) {
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

		callback(kv.DecodeFrom(key), kv.NewValue(value))
		bytes = bytes[uint16(block.ReservedKeySize)+keySize+uint16(block.ReservedValueSize)+valueSize:]
	}
	return &WAL{file: file}, nil
}

func (wal *WAL) Append(key kv.Key, value kv.Value) error {
	buffer := make([]byte, key.EncodedSizeInBytes()+value.SizeInBytes()+block.ReservedKeySize+block.ReservedValueSize)

	binary.LittleEndian.PutUint16(buffer, uint16(key.EncodedSizeInBytes()))
	copy(buffer[block.ReservedKeySize:], key.EncodedBytes())

	binary.LittleEndian.PutUint16(buffer[block.ReservedKeySize+key.EncodedSizeInBytes():], uint16(value.SizeInBytes()))
	copy(buffer[block.ReservedKeySize+key.EncodedSizeInBytes()+block.ReservedValueSize:], value.Bytes())

	_, err := wal.file.Write(buffer)
	return err
}

func (wal *WAL) Sync() error {
	return wal.file.Sync()
}

func (wal WAL) Close() {
	_ = wal.file.Close()
}
