package internal

import (
	"encoding/binary"
	"go-lsm/kv"
)

/**
 * Binary Layout of a Node at 'nodeOffset' in Arena:
 *
 *  0             2             6                   10                10 + KeyLen        Total Size
 * +-------------+-------------+-------------------+-----------------+------------------+
 * | Key Length  | Val Length  | Next Node Offset  | Key Payload     | Value Payload    |
 * | (uint16)    | (uint32)    | (uint32)          | (KeyLen bytes)  | (ValLen bytes)   |
 * | [ 2 Bytes ] | [ 4 Bytes ] | [ 4 Bytes ]       |                 |                  |
 * +-------------+-------------+-------------------+-----------------+------------------+
 * |<-------- Fixed Header (10 Bytes) ------------>|
 */

// Node represents a handle to a record stored at 'offset' inside an Arena.
type Node struct {
	offset uint32
	arena  *Arena
}

// newNode allocates space in the arena and serializes the header, key, and value.
func newNode(arena *Arena, key kv.Key, value kv.Value) (Node, error) {
	encodedKey := key.EncodedBytes()
	valBytes := value.Bytes()

	keyLength := uint16(len(encodedKey))
	valLength := uint32(len(valBytes))
	nodeSize := nodeHeaderSize + uint32(keyLength) + valLength

	offset, err := arena.allocate(nodeSize)
	if err != nil {
		return Node{offset: nullOffset, arena: arena}, err
	}

	// 1. Serialize fixed header fields
	binary.LittleEndian.PutUint16(arena.buffer[offset+keyLenOffset:offset+keyLenOffset+keyLengthSize], keyLength)
	binary.LittleEndian.PutUint32(arena.buffer[offset+valLenOffset:offset+valLenOffset+valLengthSize], valLength)
	binary.LittleEndian.PutUint32(arena.buffer[offset+nextOffsetOffset:offset+nextOffsetOffset+nextOffsetSize], nullOffset)

	// 2. Serialize variable payload fields (Key bytes followed by Value bytes)
	keyStart := offset + nodeHeaderSize
	keyEnd := keyStart + uint32(keyLength)
	valEnd := keyEnd + valLength

	copy(arena.buffer[keyStart:keyEnd], encodedKey)
	copy(arena.buffer[keyEnd:valEnd], valBytes)

	return Node{offset: offset, arena: arena}, nil
}

// nodeAt creates a Node handle referencing an existing offset in the arena.
func nodeAt(arena *Arena, offset uint32) Node {
	return Node{offset: offset, arena: arena}
}

// IsNull returns true if the node represents a null/nil pointer.
func (node Node) IsNull() bool {
	return node.offset == nullOffset
}

// Key decodes and returns the versioned kv.Key from the node.
func (node Node) Key() kv.Key {
	if node.IsNull() {
		return kv.EmptyKey
	}
	keyLength := binary.LittleEndian.Uint16(node.arena.buffer[node.offset+keyLenOffset : node.offset+keyLenOffset+keyLengthSize])
	keyBytes := node.arena.bytes(node.offset+nodeHeaderSize, uint32(keyLength))
	return kv.DecodeFrom(keyBytes)
}

// Value extracts and returns the kv.Value from the node.
func (node Node) Value() kv.Value {
	if node.IsNull() {
		return kv.EmptyValue
	}
	keyLen := uint32(binary.LittleEndian.Uint16(node.arena.buffer[node.offset+keyLenOffset : node.offset+keyLenOffset+keyLengthSize]))
	valueLength := binary.LittleEndian.Uint32(node.arena.buffer[node.offset+valLenOffset : node.offset+valLenOffset+valLengthSize])
	valueStart := node.offset + nodeHeaderSize + keyLen
	valueBytes := node.arena.bytes(valueStart, valueLength)
	return kv.NewValue(valueBytes)
}

// Next returns the handle of the next connected Node in the linked list.
func (node Node) Next() Node {
	if node.IsNull() {
		return Node{offset: nullOffset, arena: node.arena}
	}
	nextOffset := binary.LittleEndian.Uint32(node.arena.buffer[node.offset+nextOffsetOffset : node.offset+nextOffsetOffset+nextOffsetSize])
	return nodeAt(node.arena, nextOffset)
}

// SetNext updates the next pointer offset stored in this node's header.
func (node Node) SetNext(next Node) {
	if !node.IsNull() {
		binary.LittleEndian.PutUint32(
			node.arena.buffer[node.offset+nextOffsetOffset:node.offset+nextOffsetOffset+nextOffsetSize], next.offset,
		)
	}
}
