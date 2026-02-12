// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package str

import (
	"fmt"
	"strconv"
	"strings"
)

// String represents a string value with encoding optimization
type String struct {
	value  string
	isInt  bool
	intVal int64
}

// NewString creates a new string
func NewString(s string) *String {
	str := &String{value: s}
	str.tryEncodeInt()
	return str
}

// NewStringFromInt creates a string from an integer
func NewStringFromInt(i int64) *String {
	return &String{
		value:  strconv.FormatInt(i, 10),
		isInt:  true,
		intVal: i,
	}
}

// String returns the string value
func (s *String) String() string {
	return s.value
}

// Bytes returns the string value as bytes
func (s *String) Bytes() []byte {
	return []byte(s.value)
}

// Int returns the integer value if encoded as int
func (s *String) Int() (int64, bool) {
	return s.intVal, s.isInt
}

// Set sets the string value
func (s *String) Set(v string) {
	s.value = v
	s.tryEncodeInt()
}

// SetInt sets the integer value
func (s *String) SetInt(i int64) {
	s.value = strconv.FormatInt(i, 10)
	s.isInt = true
	s.intVal = i
}

// Append appends a string to the current value
func (s *String) Append(v string) int {
	s.value += v
	s.tryEncodeInt()
	return len(s.value)
}

// GetRange returns a substring
func (s *String) GetRange(start, end int) string {
	runes := []rune(s.value)
	length := len(runes)

	// Handle negative indices
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	// Clamp indices
	if start >= length {
		return ""
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return ""
	}

	return string(runes[start : end+1])
}

// SetRange sets a substring at a specific position
func (s *String) SetRange(offset int, v string) int {
	runes := []rune(s.value)

	if offset < 0 {
		offset = 0
	}

	// Extend string if needed
	if offset >= len(runes) {
		newRunes := make([]rune, offset+len(v))
		copy(newRunes, runes)
		runes = newRunes
	}

	vRunes := []rune(v)
	for i, r := range vRunes {
		if offset+i < len(runes) {
			runes[offset+i] = r
		} else {
			runes = append(runes, r)
		}
	}

	s.value = string(runes)
	s.tryEncodeInt()
	return len(s.value)
}

// Incr increments the integer value by 1
func (s *String) Incr() (int64, error) {
	return s.IncrBy(1)
}

// Decr decrements the integer value by 1
func (s *String) Decr() (int64, error) {
	return s.IncrBy(-1)
}

// IncrBy increments the integer value by delta
func (s *String) IncrBy(delta int64) (int64, error) {
	val := s.intVal
	if !s.isInt {
		// Parse as integer
		var err error
		val, err = strconv.ParseInt(s.value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}

	newVal := val + delta
	// Check for overflow
	if (delta > 0 && newVal < val) || (delta < 0 && newVal > val) {
		return 0, fmt.Errorf("increment would overflow")
	}

	s.SetInt(newVal)
	return newVal, nil
}

// IncrByFloat increments the value by float delta
func (s *String) IncrByFloat(delta float64) (float64, error) {
	val, err := strconv.ParseFloat(s.value, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not a valid float")
	}

	newVal := val + delta
	s.value = strconv.FormatFloat(newVal, 'f', -1, 64)
	s.isInt = false
	s.intVal = 0

	return newVal, nil
}

// StrLen returns the length of the string
func (s *String) StrLen() int {
	return len(s.value)
}

// tryEncodeInt tries to encode the string as an integer
func (s *String) tryEncodeInt() {
	if i, err := strconv.ParseInt(s.value, 10, 64); err == nil {
		s.isInt = true
		s.intVal = i
	} else {
		s.isInt = false
		s.intVal = 0
	}
}

// GetBit returns the bit value at offset
func (s *String) GetBit(offset int) byte {
	if offset < 0 {
		return 0
	}

	byteIndex := offset / 8
	bitIndex := offset % 8

	if byteIndex >= len(s.value) {
		return 0
	}

	b := s.value[byteIndex]
	return (b >> (7 - bitIndex)) & 1
}

// SetBit sets the bit value at offset and returns the old value
func (s *String) SetBit(offset int, value int) byte {
	if offset < 0 {
		return 0
	}

	byteIndex := offset / 8
	bitIndex := offset % 8

	// Extend string if needed
	for len(s.value) <= byteIndex {
		s.value += "\x00"
	}

	b := s.value[byteIndex]
	oldValue := (b >> (7 - bitIndex)) & 1

	if value != 0 {
		b |= 1 << (7 - bitIndex)
	} else {
		b &= ^(1 << (7 - bitIndex))
	}

	bytes := []byte(s.value)
	bytes[byteIndex] = b
	s.value = string(bytes)

	s.tryEncodeInt()

	return oldValue
}

// BitCount counts the number of bits set in a range
func (s *String) BitCount(start, end int) int {
	runes := []rune(s.value)
	length := len(runes)

	// Handle negative indices
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	// Clamp indices
	if start >= length {
		return 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return 0
	}

	count := 0
	for i := start; i <= end; i++ {
		b := s.value[i]
		for b != 0 {
			count += int(b & 1)
			b >>= 1
		}
	}

	return count
}

// BitOp performs bitwise operations on strings
func BitOp(op string, dest string, srcs ...*String) (int, error) {
	if len(srcs) == 0 {
		return 0, fmt.Errorf("no source keys")
	}

	var result []byte

	switch strings.ToUpper(op) {
	case "AND", "OR", "XOR":
		// These operations require at least 2 sources
		if len(srcs) < 2 {
			return 0, fmt.Errorf("BITOP %s requires at least 2 source keys", op)
		}

		// Find the maximum length
		maxLen := 0
		for _, src := range srcs {
			if len(src.value) > maxLen {
				maxLen = len(src.value)
			}
		}

		result = make([]byte, maxLen)

		for i := 0; i < maxLen; i++ {
			var b byte
			switch strings.ToUpper(op) {
			case "AND":
				b = 0xFF
				for _, src := range srcs {
					if i < len(src.value) {
						b &= src.value[i]
					} else {
						b &= 0
					}
				}
			case "OR":
				b = 0
				for _, src := range srcs {
					if i < len(src.value) {
						b |= src.value[i]
					}
				}
			case "XOR":
				b = 0
				for _, src := range srcs {
					if i < len(src.value) {
						b ^= src.value[i]
					}
				}
			}
			result[i] = b
		}

	case "NOT":
		if len(srcs) != 1 {
			return 0, fmt.Errorf("BITOP NOT requires exactly 1 source key")
		}

		src := srcs[0]
		result = make([]byte, len(src.value))
		for i := 0; i < len(src.value); i++ {
			result[i] = ^src.value[i]
		}

	default:
		return 0, fmt.Errorf("unknown BITOP operation: %s", op)
	}

	return len(result), nil
}
