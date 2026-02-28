// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package database

import (
	"sync"
	"sync/atomic"
)

// Dict is a generic hash table implementation with incremental rehash
type Dict struct {
	mu sync.RWMutex

	// Hash tables for rehashing
	ht [2]*dictTable

	// Rehash index: -1 means not rehashing
	rehashIdx int

	// Iterator count
	iterators uint32

	// Total number of keys
	size int
}

// dictTable is a single hash table
type dictTable struct {
	table    []*dictEntry
	size     uint64 // Number of slots
	sizemask uint64 // size - 1, used for modulo
	used     uint64 // Number of used slots
}

// dictEntry represents a key-value pair in the hash table
type dictEntry struct {
	key   string
	value interface{}
	next  *dictEntry // For chaining
}

const (
	// Initial hash table size
	dictInitialSize = 4

	// When rehashing, move this many entries per operation
	dictRehashSteps = 1

	// Force rehash if used/size ratio exceeds this
	dictForceResizeRatio = 5
)

// NewDict creates a new dictionary
func NewDict() *Dict {
	d := &Dict{
		rehashIdx: -1,
	}

	d.ht[0] = &dictTable{
		table:    make([]*dictEntry, dictInitialSize),
		size:     dictInitialSize,
		sizemask: dictInitialSize - 1,
		used:     0,
	}

	d.ht[1] = &dictTable{
		table:    nil,
		size:     0,
		sizemask: 0,
		used:     0,
	}

	return d
}

// Len returns the number of entries in the dictionary
func (d *Dict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.size
}

// Get returns the value for a key
func (d *Dict) Get(key string) (interface{}, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.size == 0 {
		return nil, false
	}

	// Try both tables if rehashing
	for i := 0; i < 2; i++ {
		if d.ht[i].used == 0 {
			continue
		}

		idx := d.hash(key, uint64(d.ht[i].sizemask))
		ent := d.ht[i].table[idx]

		for ent != nil {
			if ent.key == key {
				return ent.value, true
			}
			ent = ent.next
		}

		// If not rehashing, don't check table 1
		if !d.isRehashing() {
			break
		}
	}

	return nil, false
}

// Set sets a key-value pair
func (d *Dict) Set(key string, value interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Perform incremental rehash if needed
	if d.isRehashing() {
		d.rehash(1)
	}

	// Try to update existing key
	for i := 0; i < 2; i++ {
		if d.ht[i].used == 0 {
			continue
		}

		idx := d.hash(key, uint64(d.ht[i].sizemask))
		ent := d.ht[i].table[idx]

		for ent != nil {
			if ent.key == key {
				// Key exists, update value
				ent.value = value
				return
			}
			ent = ent.next
		}

		if !d.isRehashing() {
			break
		}
	}

	// Key doesn't exist, add new entry
	d.addToHT(0, key, value)
	d.size++

	// Check if we need to expand
	if d.ht[0].used >= d.ht[0].size {
		d.expand()
	}
}

// SetNX sets a key-value pair only if key doesn't exist
func (d *Dict) SetNX(key string, value interface{}) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.isRehashing() {
		d.rehash(1)
	}

	// Check if key exists
	for i := 0; i < 2; i++ {
		if d.ht[i].used == 0 {
			continue
		}

		idx := d.hash(key, uint64(d.ht[i].sizemask))
		ent := d.ht[i].table[idx]

		for ent != nil {
			if ent.key == key {
				return false // Key exists
			}
			ent = ent.next
		}

		if !d.isRehashing() {
			break
		}
	}

	// Add new entry
	d.addToHT(0, key, value)
	d.size++

	if d.ht[0].used >= d.ht[0].size {
		d.expand()
	}

	return true
}

// Delete removes a key from the dictionary
func (d *Dict) Delete(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.isRehashing() {
		d.rehash(1)
	}

	for i := 0; i < 2; i++ {
		if d.ht[i].used == 0 {
			continue
		}

		idx := d.hash(key, uint64(d.ht[i].sizemask))
		ent := d.ht[i].table[idx]

		var prev *dictEntry
		for ent != nil {
			if ent.key == key {
				// Found, remove it
				if prev == nil {
					d.ht[i].table[idx] = ent.next
				} else {
					prev.next = ent.next
				}
				d.ht[i].used--
				d.size--
				return true
			}
			prev = ent
			ent = ent.next
		}

		if !d.isRehashing() {
			break
		}
	}

	return false
}

// Exists checks if a key exists
func (d *Dict) Exists(key string) bool {
	_, ok := d.Get(key)
	return ok
}

// RandomKey returns a random key from the dictionary
func (d *Dict) RandomKey() (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.size == 0 {
		return "", false
	}

	// Try to find a non-empty slot
	maxTries := 100
	for try := 0; try < maxTries; try++ {
		// Check table 0
		if d.ht[0].used > 0 {
			idx := fastrandn(uint64(d.ht[0].size))
			ent := d.ht[0].table[idx]
			if ent != nil {
				return ent.key, true
			}
		}

		// Check table 1 if rehashing
		if d.isRehashing() && d.ht[1].used > 0 {
			idx := fastrandn(uint64(d.ht[1].size))
			ent := d.ht[1].table[idx]
			if ent != nil {
				return ent.key, true
			}
		}
	}

	// Fallback: iterate to find a key
	return d.iterateForRandomKey()
}

func (d *Dict) iterateForRandomKey() (string, bool) {
	// Check table 0
	for i := uint64(0); i < d.ht[0].size; i++ {
		ent := d.ht[0].table[i]
		if ent != nil {
			return ent.key, true
		}
	}

	// Check table 1 if rehashing
	if d.isRehashing() {
		for i := uint64(0); i < d.ht[1].size; i++ {
			ent := d.ht[1].table[i]
			if ent != nil {
				return ent.key, true
			}
		}
	}

	return "", false
}

// Keys returns all keys in the dictionary
func (d *Dict) Keys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	keys := make([]string, 0, d.size)

	for i := 0; i < 2; i++ {
		if d.ht[i].table == nil {
			continue
		}

		for j := uint64(0); j < d.ht[i].size; j++ {
			ent := d.ht[i].table[j]
			for ent != nil {
				keys = append(keys, ent.key)
				ent = ent.next
			}
		}

		if !d.isRehashing() {
			break
		}
	}

	return keys
}

// Clear removes all entries from the dictionary
func (d *Dict) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ht[0] = &dictTable{
		table:    make([]*dictEntry, dictInitialSize),
		size:     dictInitialSize,
		sizemask: dictInitialSize - 1,
		used:     0,
	}

	d.ht[1] = &dictTable{
		table:    nil,
		size:     0,
		sizemask: 0,
		used:     0,
	}

	d.rehashIdx = -1
	d.size = 0
}

// isRehashing returns true if the dictionary is rehashing
func (d *Dict) isRehashing() bool {
	return d.rehashIdx != -1
}

// expand expands the hash table
func (d *Dict) expand() {
	// Calculate new size (next power of 2)
	newSize := d.ht[0].size * 2
	if newSize > dictForceResizeRatio*dictInitialSize && d.ht[0].used < dictForceResizeRatio {
		return
	}

	d.rehashTo(int(newSize))
}

// rehashTo starts rehashing to a new table of the given size
func (d *Dict) rehashTo(size int) {
	// Initialize table 1
	d.ht[1] = &dictTable{
		table:    make([]*dictEntry, size),
		size:     uint64(size),
		sizemask: uint64(size) - 1,
		used:     0,
	}

	d.rehashIdx = 0
}

// rehash performs incremental rehashing
func (d *Dict) rehash(steps int) {
	if d.rehashIdx == -1 {
		return
	}

	for ; steps > 0; steps-- {
		// Check if rehashing is complete
		if d.ht[0].used == 0 {
			// Swap tables
			d.ht[0] = d.ht[1]
			d.ht[1] = &dictTable{
				table:    nil,
				size:     0,
				sizemask: 0,
				used:     0,
			}
			d.rehashIdx = -1
			return
		}

		// Find next non-empty slot in table 0
		for uint64(d.rehashIdx) < d.ht[0].size && d.ht[0].table[d.rehashIdx] == nil {
			d.rehashIdx++
		}
		// Check if we've gone past the end
		if uint64(d.rehashIdx) >= d.ht[0].size {
			// Shouldn't happen if used > 0, but return safely
			return
		}

		// Move all entries from this slot
		ent := d.ht[0].table[d.rehashIdx]
		for ent != nil {
			next := ent.next

			// Calculate new index
			idx := d.hash(ent.key, d.ht[1].sizemask)

			// Add to table 1
			ent.next = d.ht[1].table[idx]
			d.ht[1].table[idx] = ent
			d.ht[1].used++

			ent = next
			d.ht[0].used--
		}

		// Clear the slot
		d.ht[0].table[d.rehashIdx] = nil
		d.rehashIdx++
	}
}

// addToHT adds an entry to the specified hash table
func (d *Dict) addToHT(htIdx int, key string, value interface{}) {
	idx := d.hash(key, d.ht[htIdx].sizemask)

	ent := &dictEntry{
		key:   key,
		value: value,
		next:  d.ht[htIdx].table[idx],
	}

	d.ht[htIdx].table[idx] = ent
	d.ht[htIdx].used++
}

// hash calculates the hash of a key
func (d *Dict) hash(key string, mask uint64) uint64 {
	h := murmur64([]byte(key))
	return h & mask
}

// murmur64 is a 64-bit MurmurHash2 implementation
func murmur64(data []byte) uint64 {
	const (
		m = uint64(0xc6a4a7935bd1e995)
		r = 47
	)

	var h uint64 = ^uint64(0) // seed

	length := len(data)
	nblocks := length / 8

	for i := 0; i < nblocks; i++ {
		k := uint64(data[i*8]) | uint64(data[i*8+1])<<8 |
			uint64(data[i*8+2])<<16 | uint64(data[i*8+3])<<24 |
			uint64(data[i*8+4])<<32 | uint64(data[i*8+5])<<40 |
			uint64(data[i*8+6])<<48 | uint64(data[i*8+7])<<56

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	tail := data[nblocks*8:]

	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}

// fastrandn returns a random number in [0, n)
func fastrandn(n uint64) uint64 {
	// Simple xorshift RNG
	seed := atomic.LoadUint64(&randSeed)
	for {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		if atomic.CompareAndSwapUint64(&randSeed, seed, seed) {
			break
		}
	}
	return seed % n
}

var randSeed uint64 = 1

// Iterator returns an iterator for the dictionary
func (d *Dict) Iterator() *DictIterator {
	d.mu.Lock()
	atomic.AddUint32(&d.iterators, 1)
	d.mu.Unlock()

	return &DictIterator{
		dict:   d,
		table:  0,
		bucket: 0,
		ent:    nil,
	}
}

// DictIterator iterates over dictionary entries
type DictIterator struct {
	dict   *Dict
	table  int
	bucket uint64
	ent    *dictEntry
}

// Next moves to the next entry
func (it *DictIterator) Next() bool {
	if it.ent != nil && it.ent.next != nil {
		it.ent = it.ent.next
		return true
	}

	it.dict.mu.Lock()
	defer it.dict.mu.Unlock()

	for {
		if it.table >= 2 {
			return false
		}

		table := it.dict.ht[it.table]
		if table == nil || table.table == nil {
			it.table++
			it.bucket = 0
			continue
		}

		if it.bucket >= table.size {
			it.table++
			it.bucket = 0
			continue
		}

		it.bucket++
		if it.bucket < table.size {
			it.ent = table.table[it.bucket]
			if it.ent != nil {
				return true
			}
		}
	}
}

// Entry returns the current entry
func (it *DictIterator) Entry() (string, interface{}) {
	if it.ent == nil {
		return "", nil
	}
	return it.ent.key, it.ent.value
}

// Close closes the iterator
func (it *DictIterator) Close() {
	if it.dict != nil {
		atomic.AddUint32(&it.dict.iterators, ^uint32(0))
		it.dict = nil
	}
}
