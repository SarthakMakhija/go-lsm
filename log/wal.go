package log

import (
	"encoding/binary"
	"fmt"
	"go-lsm/kv"
	"go-lsm/table/block"
	"io"
	"log"
	"os"
	"path/filepath"
)

// WAL is a write-ahead log. It contains a pointer to the file on disk.
type WAL struct {
	file *os.File
}

// NewWALForId creates a new instance of WAL for the specified memtable id and a directory path.
// This implementation has WAL for each memtable.
func NewWALForId(id uint64, walDirectoryPath string) (*WAL, error) {
	return newWAL(filepath.Join(walDirectoryPath, fmt.Sprintf("%v.wal", id)))
}

// Recover recovers from WAL.
// Recovery involves the following:
// 1) Reading the file in READONLY & APPEND mode.
// 2) Reading the whole file.
// 3) Iterating through the file buffer (/bytes) and decoding each the bytes to get kv.Key and kv.Value.
// 4) Invoking the provided callback with kv.Key and kv.Value.
// There are a few approaches in terms of reading the WAL:
//  1. Read the whole file.
//  2. Implement a page-aligned WAL, which means the data in the WAL will be aligned to the page (say, 4KB application page).
//     Read page by page. This implementation will however result in fragmentation in WAL (during writing).
//  3. Read as per the encoding of data. Instead of reading the whole file, multiple file reads will be issued, to read the key size,
//     key, value size and value.
//  4. Implement WAL as a memory-mapped file.
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

// Append appends the kv.Key, kv.Value pair to WAL.
// It is important to note that WAL contained versioned keys.
// The encoding of a kv.Key, kv.Value pair in WAL looks like:
/*
 --------------------------------------------------------
| 2 bytes key size | kv.Key | 2 bytes value size | Value |
 --------------------------------------------------------
*/
func (wal *WAL) Append(key kv.Key, value kv.Value) error {
	buffer := make([]byte, key.EncodedSizeInBytes()+value.SizeInBytes()+block.ReservedKeySize+block.ReservedValueSize)

	binary.LittleEndian.PutUint16(buffer, uint16(key.EncodedSizeInBytes()))
	copy(buffer[block.ReservedKeySize:], key.EncodedBytes())

	binary.LittleEndian.PutUint16(buffer[block.ReservedKeySize+key.EncodedSizeInBytes():], uint16(value.SizeInBytes()))
	copy(buffer[block.ReservedKeySize+key.EncodedSizeInBytes()+block.ReservedValueSize:], value.Bytes())

	_, err := wal.file.Write(buffer)
	return err
}

// Sync performs a fsync operation on WAL.
func (wal *WAL) Sync() error {
	return wal.file.Sync()
}

// DeleteFile deletes the WAL (/WAL file).
func (wal *WAL) DeleteFile() {
	err := os.Remove(wal.file.Name())
	if err != nil {
		log.Printf("failed to delete WAL log file %v: %v", wal.file.Name(), err)
	}
}

// Close closes the WAL.
func (wal *WAL) Close() {
	_ = wal.file.Close()
}

// Path returns the WAL path.
func (wal *WAL) Path() (string, error) {
	return filepath.Abs(wal.file.Name())
}

// newWAL creates a new instance of WAL.
// WAL file is opened in READ-WRITE and APPEND mode.
func newWAL(path string) (*WAL, error) {
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
