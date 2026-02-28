// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"math"
	"time"

	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/zset"
)

// RDB opcodes
const (
	OpcodeEOF        = 0xFF
	OpcodeSelectDB   = 0xFE
	OpcodeResizeDB   = 0xFB
	OpcodeAux        = 0xFA
	OpcodeExpireTime = 0xFD
	OpcodeExpireMS   = 0xFC
)

// RDB value types (must match database.ObjType order)
const (
	TypeString = 0
	TypeList   = 1
	TypeSet    = 2
	TypeZSet   = 3
	TypeHash   = 4
	TypeZSet2  = 5 // ZSet with double scores
)

// RDB version
const (
	RDBVersion = 9
	Magic      = "REDIS"
)

// Encoder encodes database state to RDB format
type Encoder struct {
	w   *bufio.Writer
	crc  hash.Hash64
	pos  int // track position for CRC
}

// NewEncoder creates a new RDB encoder
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w:   bufio.NewWriter(w),
		crc: crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// Encode writes the database to RDB format
func (e *Encoder) Encode(dbs []*database.DB) error {
	// Write magic string and version
	if err := e.writeHeader(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write each database
	for i, db := range dbs {
		if err := e.writeDatabase(i, db); err != nil {
			return fmt.Errorf("failed to write database %d: %w", i, err)
		}
	}

	// Write EOF
	if err := e.writeEOF(); err != nil {
		return fmt.Errorf("failed to write EOF: %w", err)
	}

	// Flush buffer
	if err := e.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	return nil
}

// writeHeader writes the RDB file header
func (e *Encoder) writeHeader() error {
	// Write magic string "REDIS"
	if _, err := e.w.WriteString(Magic); err != nil {
		return err
	}
	e.updateCRC([]byte("REDIS"))

	// Write RDB version (4 bytes, big endian)
	version := make([]byte, 4)
	version[0] = byte(RDBVersion >> 24)
	version[1] = byte(RDBVersion >> 16)
	version[2] = byte(RDBVersion >> 8)
	version[3] = byte(RDBVersion)
	if _, err := e.w.Write(version); err != nil {
		return err
	}
	e.updateCRC(version)

	return nil
}

// writeDatabase writes a single database
func (e *Encoder) writeDatabase(dbIndex int, db *database.DB) error {
	// Get all keys
	dict := db.GetDict()
	keys := dict.Keys()
	if len(keys) == 0 {
		return nil
	}

	// Write SELECTDB opcode
	if err := e.w.WriteByte(OpcodeSelectDB); err != nil {
		return err
	}
	e.updateCRC([]byte{OpcodeSelectDB})

	// Write database number (length encoded)
	if err := e.writeLength(uint64(dbIndex)); err != nil {
		return err
	}

	// Write resize DB info (table size, key count)
	if err := e.writeLength(uint64(len(keys))); err != nil {
		return err
	}
	if err := e.writeLength(uint64(len(keys))); err != nil {
		return err
	}

	// Get expires dict
	expiresDict := db.GetExpiresDict()

	// Write all key-value pairs
	for _, key := range keys {
		// Get object
		obj, ok := dict.Get(key)
		if !ok {
			continue
		}

		dataObj, ok := obj.(*database.Object)
		if !ok {
			continue
		}

		// Check expiration
		if exp, ok := expiresDict.Get(key); ok {
			expireTime := exp.(int64)
			// Only write expiration if in the future
			if expireTime > time.Now().Unix() {
				if err := e.writeExpireTime(expireTime); err != nil {
					return err
				}
			}
		}

		// Write value based on type
		if err := e.writeValue(key, dataObj); err != nil {
			return fmt.Errorf("failed to write key %s: %w", key, err)
		}
	}

	return nil
}

// writeExpireTime writes the expiration time in milliseconds
func (e *Encoder) writeExpireTime(expireTime int64) error {
	// Use millisecond precision (newer format)
	if err := e.w.WriteByte(OpcodeExpireMS); err != nil {
		return err
	}
	e.updateCRC([]byte{OpcodeExpireMS})

	// Write 8 byte millisecond timestamp (little endian)
	expireMS := expireTime * 1000
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(expireMS))
	if _, err := e.w.Write(bytes); err != nil {
		return err
	}
	e.updateCRC(bytes)

	return nil
}

// writeValue writes a value based on its type
func (e *Encoder) writeValue(key string, obj *database.Object) error {
	// Write key
	if err := e.writeString(key); err != nil {
		return err
	}

	// Write value based on type
	switch obj.Type {
	case database.ObjTypeString:
		return e.writeStringValue(obj)
	case database.ObjTypeHash:
		return e.writeHashValue(obj)
	case database.ObjTypeList:
		return e.writeListValue(obj)
	case database.ObjTypeSet:
		return e.writeSetValue(obj)
	case database.ObjTypeZSet:
		return e.writeZSetValue(obj)
	default:
		return fmt.Errorf("unsupported type: %d", obj.Type)
	}
}

// writeStringValue writes a string value
func (e *Encoder) writeStringValue(obj *database.Object) error {
	// Write type opcode
	if err := e.w.WriteByte(TypeString); err != nil {
		return err
	}
	e.updateCRC([]byte{TypeString})

	// Get string value
	val, ok := obj.Ptr.(string)
	if !ok {
		// Try int64
		if i, ok := obj.Ptr.(int64); ok {
			val = fmt.Sprintf("%d", i)
		} else {
			return errors.New("invalid string value")
		}
	}

	// Write string value
	return e.writeString(val)
}

// writeHashValue writes a hash value
func (e *Encoder) writeHashValue(obj *database.Object) error {
	// Write type opcode
	if err := e.w.WriteByte(TypeHash); err != nil {
		return err
	}
	e.updateCRC([]byte{TypeHash})

	// Get hash interface
	hashInt, ok := obj.GetHash()
	if !ok {
		return errors.New("invalid hash value")
	}

	// Type assert to get the actual hash implementation
	type getAller interface {
		GetAllMap() map[string]string
	}

	hashImpl, ok := hashInt.(getAller)
	if !ok {
		return errors.New("hash does not implement GetAllMap")
	}

	hashMap := hashImpl.GetAllMap()

	// Write length
	if err := e.writeLength(uint64(len(hashMap))); err != nil {
		return err
	}

	// Write field-value pairs
	for field, value := range hashMap {
		if err := e.writeString(field); err != nil {
			return err
		}
		if err := e.writeString(value); err != nil {
			return err
		}
	}

	return nil
}

// writeListValue writes a list value
func (e *Encoder) writeListValue(obj *database.Object) error {
	// Write type opcode
	if err := e.w.WriteByte(TypeList); err != nil {
		return err
	}
	e.updateCRC([]byte{TypeList})

	// Get list interface
	type listAller interface {
		ToSlice() []string
	}

	listPtr, ok := obj.Ptr.(interface{})
	if !ok {
		return errors.New("invalid list value")
	}

	listImpl, ok := listPtr.(listAller)
	if !ok {
		return errors.New("list does not implement ToSlice")
	}

	list := listImpl.ToSlice()

	// Write length
	if err := e.writeLength(uint64(len(list))); err != nil {
		return err
	}

	// Write elements
	for _, elem := range list {
		if err := e.writeString(elem); err != nil {
			return err
		}
	}

	return nil
}

// writeSetValue writes a set value
func (e *Encoder) writeSetValue(obj *database.Object) error {
	// Write type opcode
	if err := e.w.WriteByte(TypeSet); err != nil {
		return err
	}
	e.updateCRC([]byte{TypeSet})

	// Get set interface
	setInt, ok := obj.GetSet()
	if !ok {
		return errors.New("invalid set value")
	}

	// Type assert to get members
	type setAller interface {
		Members() []string
	}

	setImpl, ok := setInt.(setAller)
	if !ok {
		return errors.New("set does not implement Members")
	}

	members := setImpl.Members()

	// Write length
	if err := e.writeLength(uint64(len(members))); err != nil {
		return err
	}

	// Write elements
	for _, elem := range members {
		if err := e.writeString(elem); err != nil {
			return err
		}
	}

	return nil
}

// writeZSetValue writes a sorted set value
func (e *Encoder) writeZSetValue(obj *database.Object) error {
	// Write type opcode (ZSet2 with double scores)
	if err := e.w.WriteByte(TypeZSet2); err != nil {
		return err
	}
	e.updateCRC([]byte{TypeZSet2})

	// Get zset interface
	zsetInt, ok := obj.GetZSet()
	if !ok {
		return errors.New("invalid zset value")
	}

	// Type assert to actual ZSet type
	zs, ok := zsetInt.(*zset.ZSet)
	if !ok {
		return errors.New("zset is not *zset.ZSet type")
	}

	// Get all elements (rank 0 to -1 means all)
	members := zs.Range(0, -1)

	// Write length
	if err := e.writeLength(uint64(len(members))); err != nil {
		return err
	}

	// Write member-score pairs
	for _, zm := range members {
		// Write member
		if err := e.writeString(zm.Member); err != nil {
			return err
		}

		// Write score (double, little endian)
		scoreBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(scoreBytes, math.Float64bits(zm.Score))
		if _, err := e.w.Write(scoreBytes); err != nil {
			return err
		}
		e.updateCRC(scoreBytes)
	}

	return nil
}

// writeString writes a string with length encoding
func (e *Encoder) writeString(s string) error {
	// Write string length
	if err := e.writeLength(uint64(len(s))); err != nil {
		return err
	}

	// Write string bytes
	bytes := []byte(s)
	if _, err := e.w.Write(bytes); err != nil {
		return err
	}
	e.updateCRC(bytes)

	return nil
}

// writeLength writes a length-encoded value
func (e *Encoder) writeLength(length uint64) error {
	var buf []byte

	switch {
	case length < 1<<6:
		// 6-bit length
		buf = []byte{byte(length)}
	case length < 1<<14:
		// 14-bit length
		buf = []byte{
			byte((length >> 8) | 0x40),
			byte(length & 0xFF),
		}
	case length < 1<<32:
		// 32-bit length
		buf = []byte{0x80}
		bytes := make([]byte, 4)
		binary.BigEndian.PutUint32(bytes, uint32(length))
		buf = append(buf, bytes...)
	default:
		// 64-bit length
		buf = []byte{0x81}
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, length)
		buf = append(buf, bytes...)
	}

	if _, err := e.w.Write(buf); err != nil {
		return err
	}
	e.updateCRC(buf)

	return nil
}

// writeEOF writes the EOF marker and CRC64
func (e *Encoder) writeEOF() error {
	// Write EOF opcode
	if err := e.w.WriteByte(OpcodeEOF); err != nil {
		return err
	}
	e.updateCRC([]byte{OpcodeEOF})

	// Write CRC64 checksum (8 bytes, little endian)
	crc := e.crc.Sum64()
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, crc)
	if _, err := e.w.Write(bytes); err != nil {
		return err
	}

	return nil
}

// updateCRC updates the CRC calculation
func (e *Encoder) updateCRC(data []byte) {
	e.pos += len(data)
	e.crc.Write(data)
}

// GetPos returns the current position in the stream
func (e *Encoder) GetPos() int {
	return e.pos
}
