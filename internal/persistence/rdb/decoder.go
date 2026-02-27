// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"math"
	"time"

	"github.com/zyhnesmr/godis/internal/database"
)

// Decoder decodes RDB format to database state
type Decoder struct {
	r   *bufio.Reader
	crc hash.Hash64
}

// NewDecoder creates a new RDB decoder
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:   bufio.NewReader(r),
		crc: crc64.New(crc64.MakeTable(crc64.ISO)),
	}
}

// Decode reads the RDB file and loads data into databases
func (d *Decoder) Decode(dbs []*database.DB) error {
	// Read and verify header
	if err := d.readHeader(); err != nil {
		return err
	}

	// Read databases until EOF
	for {
		opcode, err := d.r.ReadByte()
		if err != nil {
			return err
		}

		switch opcode {
		case OpcodeEOF:
			// Update CRC with EOF opcode
			d.crc.Write([]byte{OpcodeEOF})
			// Read and verify CRC
			return d.readCRC()
		case OpcodeSelectDB:
			// Update CRC with SELECTDB opcode
			d.crc.Write([]byte{OpcodeSelectDB})
			dbIndex, err := d.readLength()
			if err != nil {
				return err
			}
			if dbIndex >= uint64(len(dbs)) {
				return fmt.Errorf("invalid database index: %d", dbIndex)
			}
			// Read resize db info
			_, err = d.readLength()
			if err != nil {
				return err
			}
			_, err = d.readLength()
			if err != nil {
				return err
			}
			// Read key-value pairs for this database
			if err := d.readKeyValuePairs(dbs[dbIndex]); err != nil {
				return err
			}
		case OpcodeAux:
			// Skip auxiliary fields
			_, err := d.readString()
			if err != nil {
				return err
			}
			_, err = d.readString()
			if err != nil {
				return err
			}
		case OpcodeResizeDB:
			// Skip resize db info
			_, err = d.readLength()
			if err != nil {
				return err
			}
			_, err = d.readLength()
			if err != nil {
				return err
				}
		case OpcodeExpireTime, OpcodeExpireMS:
			// This should be followed by a key-value pair
			// Read the key-value pair with expiration
			return d.readKeyValuePairWithExpire(dbs[0], opcode)
		default:
			// Unknown opcode, might be a value type
			// Unread the byte and try as value type
			return fmt.Errorf("unknown opcode: %d", opcode)
		}
	}
}

// readHeader reads and verifies the RDB file header
func (d *Decoder) readHeader() error {
	// Read magic string
	magic := make([]byte, 5)
	if _, err := io.ReadFull(d.r, magic); err != nil {
		return err
	}
	if string(magic) != Magic {
		return fmt.Errorf("invalid RDB magic: %s", string(magic))
	}
	d.crc.Write(magic)

	// Read version
	versionBytes := make([]byte, 4)
	if _, err := io.ReadFull(d.r, versionBytes); err != nil {
		return err
	}
	version := binary.BigEndian.Uint32(versionBytes)
	if version > RDBVersion {
		return fmt.Errorf("unsupported RDB version: %d", version)
	}
	d.crc.Write(versionBytes)

	return nil
}

// readLength reads a length-encoded value
func (d *Decoder) readLength() (uint64, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, err
	}
	d.crc.Write([]byte{b})

	switch {
	case b&0x80 == 0:
		// 6-bit or 14-bit length
		if b&0x40 == 0 {
			// 6-bit
			return uint64(b), nil
		}
		// 14-bit
		b2, err := d.r.ReadByte()
		if err != nil {
			return 0, err
		}
		d.crc.Write([]byte{b2})
		return uint64(b&0x3F)<<8 | uint64(b2), nil

	case b&0xC0 == 0x80:
		// 32-bit or 64-bit length
		b2, err := d.r.ReadByte()
		if err != nil {
			return 0, err
		}
		d.crc.Write([]byte{b2})

		if b2&0x80 == 0 {
			// 32-bit
			bytes := make([]byte, 4)
			if _, err := io.ReadFull(d.r, bytes); err != nil {
				return 0, err
			}
			d.crc.Write(bytes)
			return uint64(binary.BigEndian.Uint32(bytes)), nil
		}
		// 64-bit
		bytes := make([]byte, 8)
		if _, err := io.ReadFull(d.r, bytes); err != nil {
			return 0, err
		}
		d.crc.Write(bytes)
		return binary.BigEndian.Uint64(bytes), nil

	default:
		return 0, fmt.Errorf("invalid length encoding: %d", b)
	}
}

// readString reads a length-encoded string
func (d *Decoder) readString() (string, error) {
	length, err := d.readLength()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	if length > 512*1024*1024 { // 512MB limit
		return "", fmt.Errorf("string too long: %d", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return "", err
	}
	d.crc.Write(data)

	return string(data), nil
}

// readKeyValuePairs reads key-value pairs into a database
func (d *Decoder) readKeyValuePairs(db *database.DB) error {
	for {
		// Peek to check for opcodes
		b, err := d.r.ReadByte()
		if err != nil {
			return err
		}

		// Check if it's an opcode
		if b == OpcodeEOF || b == OpcodeSelectDB || b == OpcodeAux || b == OpcodeResizeDB {
			// Unread the opcode
			d.r.UnreadByte()
			break
		}

		// Unread and check for expiration opcode
		d.r.UnreadByte()

		// Check for expiration
		if b == OpcodeExpireTime || b == OpcodeExpireMS {
			return d.readKeyValuePairWithExpire(db, b)
		}

		// Read key
		key, err := d.readString()
		if err != nil {
			return err
		}

		// Read value type
	 valueType, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		d.crc.Write([]byte{valueType})

		// Read value based on type
		var obj *database.Object
		switch valueType {
		case TypeString:
			obj, err = d.readStringValue()
		case TypeHash:
			obj, err = d.readHashValue()
		case TypeList:
			obj, err = d.readListValue()
		case TypeSet:
			obj, err = d.readSetValue()
		case TypeZSet, TypeZSet2:
			obj, err = d.readZSetValue(valueType)
		default:
			return fmt.Errorf("unsupported value type: %d", valueType)
		}

		if err != nil {
			return err
		}

		// Store in database
		db.Set(key, obj)

		// Check for more
		b, err = d.r.ReadByte()
		if err != nil {
			return err
		}
		d.r.UnreadByte()
		if b == OpcodeEOF || b == OpcodeSelectDB {
			break
		}
	}

	return nil
}

// readKeyValuePairWithExpire reads a key-value pair with expiration
func (d *Decoder) readKeyValuePairWithExpire(db *database.DB, opcode byte) error {
	d.crc.Write([]byte{opcode})

	var expireTime int64
	if opcode == OpcodeExpireMS {
		// Read 8 byte millisecond timestamp
		bytes := make([]byte, 8)
		if _, err := io.ReadFull(d.r, bytes); err != nil {
			return err
		}
		d.crc.Write(bytes)
		expireTime = int64(binary.LittleEndian.Uint64(bytes)) / 1000
	} else {
		// Read 4 byte second timestamp
		bytes := make([]byte, 4)
		if _, err := io.ReadFull(d.r, bytes); err != nil {
			return err
		}
		d.crc.Write(bytes)
		expireTime = int64(binary.BigEndian.Uint32(bytes))
	}

	// Read key
	key, err := d.readString()
	if err != nil {
		return err
	}

	// Read value type
	valueType, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	d.crc.Write([]byte{valueType})

	// Read value based on type
	var obj *database.Object
	switch valueType {
	case TypeString:
		obj, err = d.readStringValue()
	case TypeHash:
		obj, err = d.readHashValue()
	case TypeList:
		obj, err = d.readListValue()
	case TypeSet:
		obj, err = d.readSetValue()
	case TypeZSet, TypeZSet2:
		obj, err = d.readZSetValue(valueType)
	default:
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	if err != nil {
		return err
	}

	// Store in database
	db.Set(key, obj)

	// Set expiration if in the future
	if expireTime > time.Now().Unix() {
		db.ExpireAt(key, expireTime)
	}

	return nil
}

// readStringValue reads a string value
func (d *Decoder) readStringValue() (*database.Object, error) {
	val, err := d.readString()
	if err != nil {
		return nil, err
	}
	return database.NewStringObject(val), nil
}

// readHashValue reads a hash value
func (d *Decoder) readHashValue() (*database.Object, error) {
	length, err := d.readLength()
	if err != nil {
		return nil, err
	}

	hash := database.NewHashObject()

	for i := 0; i < int(length); i++ {
		field, err := d.readString()
		if err != nil {
			return nil, err
		}
		value, err := d.readString()
		if err != nil {
			return nil, err
		}

		hashInt, _ := hash.GetHash()
		type hashSetter interface {
			Set(field, value string) int
		}

		if h, ok := hashInt.(hashSetter); ok {
			h.Set(field, value)
		}
	}

	return hash, nil
}

// readListValue reads a list value
func (d *Decoder) readListValue() (*database.Object, error) {
	length, err := d.readLength()
	if err != nil {
		return nil, err
	}

	list := database.NewListObject()

	for i := 0; i < int(length); i++ {
		elem, err := d.readString()
		if err != nil {
			return nil, err
		}

		listInt, _ := list.Ptr.(interface{})
		type listAdder interface {
			PushRight(string) int
		}

		if l, ok := listInt.(listAdder); ok {
			l.PushRight(elem)
		}
	}

	return list, nil
}

// readSetValue reads a set value
func (d *Decoder) readSetValue() (*database.Object, error) {
	length, err := d.readLength()
	if err != nil {
		return nil, err
	}

	// Collect members first
	members := make([]string, 0, length)
	for i := 0; i < int(length); i++ {
		member, err := d.readString()
		if err != nil {
			return nil, err
		}
		members = append(members, member)
	}

	return database.NewSetObjectFromSlice(members), nil
}

// readZSetValue reads a sorted set value
func (d *Decoder) readZSetValue(_ byte) (*database.Object, error) {
	length, err := d.readLength()
	if err != nil {
		return nil, err
	}

	zset := database.NewZSetObject()
	zsetInt, _ := zset.GetZSet()

	// Type assert to get Add method
	type zsetAdder interface {
		Add(member string, score float64) int
	}

	zsetImpl, ok := zsetInt.(zsetAdder)
	if !ok {
		return nil, fmt.Errorf("zset does not implement Add")
	}

	for i := 0; i < int(length); i++ {
		// Read member
		member, err := d.readString()
		if err != nil {
			return nil, err
		}

		// Read score (8 bytes, little endian double)
		scoreBytes := make([]byte, 8)
		if _, err := io.ReadFull(d.r, scoreBytes); err != nil {
			return nil, err
		}
		d.crc.Write(scoreBytes)

		score := math.Float64frombits(binary.LittleEndian.Uint64(scoreBytes))

		// Add to zset
		zsetImpl.Add(member, score)
	}

	return zset, nil
}

// readCRC reads and verifies the CRC64 checksum
func (d *Decoder) readCRC() error {
	// Read 8 byte CRC64
	bytes := make([]byte, 8)
	if _, err := io.ReadFull(d.r, bytes); err != nil {
		return err
	}

	// Verify CRC
	crc := d.crc.Sum64()
	fileCRC := binary.LittleEndian.Uint64(bytes)
	if crc != fileCRC {
		return fmt.Errorf("CRC mismatch: calculated=%x, file=%x", crc, fileCRC)
	}

	return nil
}
