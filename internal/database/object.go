// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package database

import (
	"fmt"
	"strconv"
	"time"
)

// ObjType represents the object type
type ObjType byte

const (
	ObjTypeString ObjType = iota
	ObjTypeList
	ObjTypeHash
	ObjTypeSet
	ObjTypeZSet
	ObjTypeStream
	ObjTypeModule
)

// ObjEncoding represents the object encoding
type ObjEncoding byte

const (
	ObjEncodingRaw ObjEncoding = iota
	ObjEncodingInt
	ObjEncodingEmbstr
	ObjEncodingHashtable
	ObjEncodingLinkedList
	ObjEncodingZiplist
	ObjEncodingIntset
	ObjEncodingSkiplist
	ObjEncodingQuicklist
	ObjEncodingRadixTree
)

// String returns the string representation of the object type
func (t ObjType) String() string {
	switch t {
	case ObjTypeString:
		return "string"
	case ObjTypeList:
		return "list"
	case ObjTypeHash:
		return "hash"
	case ObjTypeSet:
		return "set"
	case ObjTypeZSet:
		return "zset"
	case ObjTypeStream:
		return "stream"
	case ObjTypeModule:
		return "module"
	default:
		return "unknown"
	}
}

// String returns the string representation of the encoding
func (e ObjEncoding) String() string {
	switch e {
	case ObjEncodingRaw:
		return "raw"
	case ObjEncodingInt:
		return "int"
	case ObjEncodingEmbstr:
		return "embstr"
	case ObjEncodingHashtable:
		return "hashtable"
	case ObjEncodingLinkedList:
		return "linkedlist"
	case ObjEncodingZiplist:
		return "ziplist"
	case ObjEncodingIntset:
		return "intset"
	case ObjEncodingSkiplist:
		return "skiplist"
	case ObjEncodingQuicklist:
		return "quicklist"
	case ObjEncodingRadixTree:
		return "radixtree"
	default:
		return "unknown"
	}
}

// ObjTypeFromString parses a string to ObjType
func ObjTypeFromString(s string) (ObjType, error) {
	switch s {
	case "string":
		return ObjTypeString, nil
	case "list":
		return ObjTypeList, nil
	case "hash":
		return ObjTypeHash, nil
	case "set":
		return ObjTypeSet, nil
	case "zset":
		return ObjTypeZSet, nil
	case "stream":
		return ObjTypeStream, nil
	default:
		return ObjTypeString, fmt.Errorf("unknown object type: %s", s)
	}
}

// Object represents a Redis object
type Object struct {
	Type     ObjType
	Encoding ObjEncoding
	Ptr      interface{}
	LRU      uint32 // LRU/LFU time field
}

// NewObject creates a new object
func NewObject(objType ObjType, encoding ObjEncoding, ptr interface{}) *Object {
	return &Object{
		Type:     objType,
		Encoding: encoding,
		Ptr:      ptr,
		LRU:      uint32(time.Now().Unix()),
	}
}

// NewStringObject creates a string object with optimal encoding
func NewStringObject(s string) *Object {
	// Try to encode as integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &Object{
			Type:     ObjTypeString,
			Encoding: ObjEncodingInt,
			Ptr:      i,
			LRU:      uint32(time.Now().Unix()),
		}
	}

	// Use embstr for short strings
	if len(s) <= 44 {
		return &Object{
			Type:     ObjTypeString,
			Encoding: ObjEncodingEmbstr,
			Ptr:      s,
			LRU:      uint32(time.Now().Unix()),
		}
	}

	// Use raw for longer strings
	return &Object{
		Type:     ObjTypeString,
		Encoding: ObjEncodingRaw,
		Ptr:      s,
		LRU:      uint32(time.Now().Unix()),
	}
}

// NewIntObject creates an integer string object
func NewIntObject(i int64) *Object {
	return &Object{
		Type:     ObjTypeString,
		Encoding: ObjEncodingInt,
		Ptr:      i,
		LRU:      uint32(time.Now().Unix()),
	}
}

// NewBulkStringObject creates a string object from bytes
func NewBulkStringObject(b []byte) *Object {
	if b == nil {
		return &Object{
			Type:     ObjTypeString,
			Encoding: ObjEncodingEmbstr,
			Ptr:      nil,
			LRU:      uint32(time.Now().Unix()),
		}
	}

	s := string(b)

	// Try to encode as integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &Object{
			Type:     ObjTypeString,
			Encoding: ObjEncodingInt,
			Ptr:      i,
			LRU:      uint32(time.Now().Unix()),
		}
	}

	// Use embstr for short strings
	if len(s) <= 44 {
		return &Object{
			Type:     ObjTypeString,
			Encoding: ObjEncodingEmbstr,
			Ptr:      s,
			LRU:      uint32(time.Now().Unix()),
		}
	}

	return &Object{
		Type:     ObjTypeString,
		Encoding: ObjEncodingRaw,
		Ptr:      s,
		LRU:      uint32(time.Now().Unix()),
	}
}

// String returns the string value
func (o *Object) String() string {
	if o == nil {
		return ""
	}

	switch v := o.Ptr.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

// Bytes returns the value as bytes
func (o *Object) Bytes() []byte {
	if o == nil {
		return nil
	}

	switch v := o.Ptr.(type) {
	case int64:
		return []byte(strconv.FormatInt(v, 10))
	case int:
		return []byte(strconv.Itoa(v))
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		return nil
	}
}

// Int returns the value as int64
func (o *Object) Int() (int64, bool) {
	if o == nil {
		return 0, false
	}

	switch v := o.Ptr.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// UpdateLRU updates the LRU/LFU timestamp
func (o *Object) UpdateLRU() {
	o.LRU = uint32(time.Now().Unix())
}

// GetLRU returns the LRU/LFU timestamp
func (o *Object) GetLRU() uint32 {
	return o.LRU
}

// IncrementLFU increments the LFU counter
// The LRU field is used for LFU: high 16 bits = time in minutes, low 8 bits = counter
func (o *Object) IncrementLFU() {
	const lfuDecayTime = 60 // seconds
	const lfuLogFactor = 10

	now := uint32(time.Now().Unix())
	counter := o.LRU & 0xff
	lastTime := o.LRU >> 8

	// Calculate minutes since last access
	minutes := (now - lastTime*lfuDecayTime) / lfuDecayTime
	if minutes > 0 {
		// Decay counter
		if minutes > lfuLogFactor {
			counter = 0
		} else {
			counter = (counter * (lfuLogFactor - minutes)) / lfuLogFactor
		}
	}

	// Increment counter with probability
	// This simulates the logarithmic counter
	if counter < 255 {
		counter++
	}

	// Pack time (in minutes) and counter
	o.LRU = (now/lfuDecayTime)<<8 | counter
}

// GetLFU returns the LFU counter
func (o *Object) GetLFU() uint8 {
	return uint8(o.LRU & 0xff)
}

// TryEncodingRaw tries to convert an object to raw encoding
func (o *Object) TryEncodingRaw() bool {
	if o.Encoding != ObjEncodingInt {
		return false
	}

	// Convert int to string
	if i, ok := o.Ptr.(int64); ok {
		o.Ptr = strconv.FormatInt(i, 10)
		o.Encoding = ObjEncodingRaw
		return true
	}

	if i, ok := o.Ptr.(int); ok {
		o.Ptr = strconv.Itoa(i)
		o.Encoding = ObjEncodingRaw
		return true
	}

	return false
}

// IsShared returns true if the object is shared (not used in this implementation)
func (o *Object) IsShared() bool {
	return false
}

// Size returns the approximate size of the object in bytes
func (o *Object) Size() int64 {
	if o == nil {
		return 0
	}

	switch v := o.Ptr.(type) {
	case int64:
		return 8
	case int:
		return 4
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	default:
		return 16 // Base object size
	}
}
