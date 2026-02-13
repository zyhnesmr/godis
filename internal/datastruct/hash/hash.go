// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hash

import (
	"strconv"
	"sync"
)

// HashEncoding represents the encoding type of a hash
type HashEncoding byte

const (
	// HashEncodingHashtable uses a Go map
	HashEncodingHashtable HashEncoding = iota
	// HashEncodingZiplist uses a more compact representation (TODO)
	HashEncodingZiplist
)

// Hash represents a Redis hash data structure
type Hash struct {
	mu       sync.RWMutex
	data     map[string]string
	encoding HashEncoding
}

// NewHash creates a new hash
func NewHash() *Hash {
	return &Hash{
		data:     make(map[string]string),
		encoding: HashEncodingHashtable,
	}
}

// NewHashFromMap creates a hash from a map
func NewHashFromMap(m map[string]string) *Hash {
	h := &Hash{
		data:     make(map[string]string, len(m)),
		encoding: HashEncodingHashtable,
	}
	for k, v := range m {
		h.data[k] = v
	}
	return h
}

// Set sets a field-value pair in the hash
func (h *Hash) Set(field, value string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, existed := h.data[field]
	h.data[field] = value

	if existed {
		return 0
	}
	return 1
}

// Get returns the value of a field
func (h *Hash) Get(field string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	val, ok := h.data[field]
	return val, ok
}

// MSet sets multiple field-value pairs
func (h *Hash) MSet(pairs map[string]string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	newFields := 0
	for field, value := range pairs {
		_, existed := h.data[field]
		h.data[field] = value
		if !existed {
			newFields++
		}
	}
	return newFields
}

// MGet gets multiple field values
func (h *Hash) MGet(fields []string) []interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]interface{}, len(fields))
	for i, field := range fields {
		if val, ok := h.data[field]; ok {
			result[i] = val
		} else {
			result[i] = nil
		}
	}
	return result
}

// Del deletes a field from the hash
func (h *Hash) Del(fields ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	deleted := 0
	for _, field := range fields {
		if _, ok := h.data[field]; ok {
			delete(h.data, field)
			deleted++
		}
	}
	return deleted
}

// Exists checks if a field exists
func (h *Hash) Exists(field string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.data[field]
	return ok
}

// Len returns the number of fields
func (h *Hash) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.data)
}

// Keys returns all field names
func (h *Hash) Keys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, len(h.data))
	for k := range h.data {
		keys = append(keys, k)
	}
	return keys
}

// Vals returns all values
func (h *Hash) Vals() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	vals := make([]string, 0, len(h.data))
	for _, v := range h.data {
		vals = append(vals, v)
	}
	return vals
}

// GetAll returns all field-value pairs
func (h *Hash) GetAll() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]string, 0, len(h.data)*2)
	for k, v := range h.data {
		result = append(result, k, v)
	}
	return result
}

// GetAllMap returns all field-value pairs as a map
func (h *Hash) GetAllMap() map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]string, len(h.data))
	for k, v := range h.data {
		result[k] = v
	}
	return result
}

// IncrBy increments a field value by delta
func (h *Hash) IncrBy(field string, delta int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	val, ok := h.data[field]
	if !ok {
		h.data[field] = strconv.FormatInt(delta, 10)
		return delta, nil
	}

	// Parse current value
	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}

	newVal := current + delta
	h.data[field] = strconv.FormatInt(newVal, 10)

	return newVal, nil
}

// IncrByFloat increments a field value by float delta
func (h *Hash) IncrByFloat(field string, delta float64) (float64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	val, ok := h.data[field]
	if !ok {
		h.data[field] = strconv.FormatFloat(delta, 'f', -1, 64)
		return delta, nil
	}

	// Parse current value
	current, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, err
	}

	newVal := current + delta
	h.data[field] = strconv.FormatFloat(newVal, 'f', -1, 64)

	return newVal, nil
}

// RandomField returns a random field
func (h *Hash) RandomField() (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.data) == 0 {
		return "", false
	}

	for k := range h.data {
		return k, true
	}

	return "", false
}

// Scan iterates over fields with cursor
func (h *Hash) Scan(cursor int, count int, pattern string) (int, []string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, count)
	dataKeys := make([]string, 0, len(h.data))

	for k := range h.data {
		dataKeys = append(dataKeys, k)
	}

	// Simple cursor-based iteration
	if cursor < 0 {
		cursor = 0
	}

	if cursor >= len(dataKeys) {
		return 0, nil
	}

	end := cursor + count
	if end > len(dataKeys) {
		end = len(dataKeys)
	}

	keys = dataKeys[cursor:end]
	newCursor := end
	if newCursor >= len(dataKeys) {
		newCursor = 0
	}

	return newCursor, keys
}

// StrLen returns the length of a field value
func (h *Hash) StrLen(field string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if val, ok := h.data[field]; ok {
		return len(val)
	}
	return 0
}

// Encoding returns the hash encoding type
func (h *Hash) Encoding() HashEncoding {
	return h.encoding
}

// Size returns the approximate memory size
func (h *Hash) Size() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	size := int64(0)
	for k, v := range h.data {
		size += int64(len(k) + len(v))
	}
	// Add overhead for map structure
	size += int64(len(h.data)) * 16
	return size
}
