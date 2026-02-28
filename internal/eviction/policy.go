// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package eviction

import (
	"fmt"
	"sync"
	"time"
)

// PolicyType represents the eviction policy type
type PolicyType int

const (
	PolicyNoEviction PolicyType = iota
	PolicyAllKeysLRU
	PolicyVolatileLRU
	PolicyAllKeysLFU
	PolicyVolatileLFU
	PolicyAllKeysRandom
	PolicyVolatileRandom
	PolicyVolatileTTL
)

// String returns the string representation of the policy
func (p PolicyType) String() string {
	switch p {
	case PolicyNoEviction:
		return "noeviction"
	case PolicyAllKeysLRU:
		return "allkeys-lru"
	case PolicyVolatileLRU:
		return "volatile-lru"
	case PolicyAllKeysLFU:
		return "allkeys-lfu"
	case PolicyVolatileLFU:
		return "volatile-lfu"
	case PolicyAllKeysRandom:
		return "allkeys-random"
	case PolicyVolatileRandom:
		return "volatile-random"
	case PolicyVolatileTTL:
		return "volatile-ttl"
	default:
		return "unknown"
	}
}

// PolicyFromString parses a string to PolicyType
func PolicyFromString(s string) (PolicyType, error) {
	switch s {
	case "noeviction":
		return PolicyNoEviction, nil
	case "allkeys-lru":
		return PolicyAllKeysLRU, nil
	case "volatile-lru":
		return PolicyVolatileLRU, nil
	case "allkeys-lfu":
		return PolicyAllKeysLFU, nil
	case "volatile-lfu":
		return PolicyVolatileLFU, nil
	case "allkeys-random":
		return PolicyAllKeysRandom, nil
	case "volatile-random":
		return PolicyVolatileRandom, nil
	case "volatile-ttl":
		return PolicyVolatileTTL, nil
	default:
		return PolicyNoEviction, fmt.Errorf("unknown eviction policy: %s", s)
	}
}

// KeyInfo holds information about a key for eviction decisions
type KeyInfo struct {
	Key        string
	LRU        uint32    // LRU timestamp or LFU data
	ExpiresAt  int64     // Expiration timestamp, 0 if no expiration
	Size       int64     // Approximate size in bytes
	LastAccess time.Time // Last access time
}

// DBAccessor provides access to database for eviction
type DBAccessor interface {
	// GetKeyInfo returns information about a key
	GetKeyInfo(key string) (*KeyInfo, bool)

	// GetRandomKey returns a random key
	GetRandomKey() (string, bool)

	// GetRandomKeyWithExpiration returns a random key that has an expiration
	GetRandomKeyWithExpiration() (string, bool)

	// GetKeysCount returns the total number of keys
	GetKeysCount() int

	// GetKeysWithExpirationCount returns the number of keys with expiration
	GetKeysWithExpirationCount() int

	// DeleteSingle deletes a single key
	DeleteSingle(key string) bool

	// GetMemoryUsage returns the current memory usage
	GetMemoryUsage() int64
}

// Policy is the interface for eviction policies
type Policy interface {
	// Name returns the policy name
	Name() string

	// Evict attempts to evict keys to free up memory
	// Returns the number of keys evicted and bytes freed
	Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64)

	// ShouldEvict returns true if eviction should be performed
	ShouldEvict(currentMemory, maxMemory int64) bool
}

// basePolicy provides common functionality for all policies
type basePolicy struct {
	policyType PolicyType
}

func (b *basePolicy) Name() string {
	return b.policyType.String()
}

func (b *basePolicy) ShouldEvict(currentMemory, maxMemory int64) bool {
	return maxMemory > 0 && currentMemory >= maxMemory
}

// NoEvictionPolicy never evicts keys
type NoEvictionPolicy struct {
	basePolicy
}

func NewNoEvictionPolicy() *NoEvictionPolicy {
	return &NoEvictionPolicy{
		basePolicy: basePolicy{policyType: PolicyNoEviction},
	}
}

func (p *NoEvictionPolicy) Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64) {
	return 0, 0 // Never evict
}

// LRUPolicy implements approximate LRU eviction
type LRUPolicy struct {
	basePolicy
	volatile bool // If true, only evict keys with expiration
	pool     *EvictionPool
}

func NewLRUPolicy(volatile bool) *LRUPolicy {
	return &LRUPolicy{
		basePolicy: basePolicy{
			policyType: func() PolicyType {
				if volatile {
					return PolicyVolatileLRU
				}
				return PolicyAllKeysLRU
			}(),
		},
		volatile: volatile,
		pool:     NewEvictionPool(16),
	}
}

func (p *LRUPolicy) Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64) {
	evicted := 0
	var freed int64

	// Fill the eviction pool with samples
	p.FillPool(db, samples)

	// Evict from the pool (best candidates first)
	for evicted < 32 && freed < bytesNeeded {
		keyInfo := p.pool.PopBest()
		if keyInfo == nil {
			break
		}

		if db.DeleteSingle(keyInfo.Key) {
			evicted++
			freed += keyInfo.Size
			if freed < 64 {
				freed = 64 // Minimum size estimate
			}
		}
	}

	return evicted, freed
}

// FillPool fills the eviction pool with candidate keys
func (p *LRUPolicy) FillPool(db DBAccessor, samples int) {
	count := db.GetKeysCount()
	if count == 0 {
		return
	}

	if samples > count {
		samples = count
	}

	now := uint32(time.Now().Unix())

	for i := 0; i < samples; i++ {
		var key string
		var ok bool

		if p.volatile {
			key, ok = db.GetRandomKeyWithExpiration()
		} else {
			key, ok = db.GetRandomKey()
		}

		if !ok {
			continue
		}

		info, ok := db.GetKeyInfo(key)
		if !ok {
			continue
		}

		// Calculate idle time (lower is better for eviction)
		idle := uint32(0)
		if info.LRU > 0 {
			idle = now - info.LRU
		}

		p.pool.Insert(key, idle, info.Size, info.ExpiresAt)
	}
}

// LFUPolicy implements approximate LFU eviction
type LFUPolicy struct {
	basePolicy
	volatile bool // If true, only evict keys with expiration
	pool     *EvictionPool
}

func NewLFUPolicy(volatile bool) *LFUPolicy {
	return &LFUPolicy{
		basePolicy: basePolicy{
			policyType: func() PolicyType {
				if volatile {
					return PolicyVolatileLFU
				}
				return PolicyAllKeysLFU
			}(),
		},
		volatile: volatile,
		pool:     NewEvictionPool(16),
	}
}

func (p *LFUPolicy) Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64) {
	evicted := 0
	var freed int64

	// Fill the eviction pool with samples
	p.FillPool(db, samples)

	// Evict from the pool (best candidates first)
	for evicted < 32 && freed < bytesNeeded {
		keyInfo := p.pool.PopBest()
		if keyInfo == nil {
			break
		}

		if db.DeleteSingle(keyInfo.Key) {
			evicted++
			freed += keyInfo.Size
			if freed < 64 {
				freed = 64
			}
		}
	}

	return evicted, freed
}

// FillPool fills the eviction pool with candidate keys for LFU
func (p *LFUPolicy) FillPool(db DBAccessor, samples int) {
	count := db.GetKeysCount()
	if count == 0 {
		return
	}

	if samples > count {
		samples = count
	}

	for i := 0; i < samples; i++ {
		var key string
		var ok bool

		if p.volatile {
			key, ok = db.GetRandomKeyWithExpiration()
		} else {
			key, ok = db.GetRandomKey()
		}

		if !ok {
			continue
		}

		info, ok := db.GetKeyInfo(key)
		if !ok {
			continue
		}

		// For LFU, lower counter means better eviction candidate
		// The LRU field contains LFU data: low 8 bits = counter
		lfuScore := 255 - uint8(info.LRU&0xff)

		p.pool.Insert(key, uint32(lfuScore), info.Size, info.ExpiresAt)
	}
}

// RandomPolicy implements random eviction
type RandomPolicy struct {
	basePolicy
	volatile bool // If true, only evict keys with expiration
}

func NewRandomPolicy(volatile bool) *RandomPolicy {
	return &RandomPolicy{
		basePolicy: basePolicy{
			policyType: func() PolicyType {
				if volatile {
					return PolicyVolatileRandom
				}
				return PolicyAllKeysRandom
			}(),
		},
		volatile: volatile,
	}
}

func (p *RandomPolicy) Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64) {
	evicted := 0
	var freed int64

	// Sample and evict random keys
	for evicted < 32 && freed < bytesNeeded {
		var key string
		var ok bool

		if p.volatile {
			key, ok = db.GetRandomKeyWithExpiration()
		} else {
			key, ok = db.GetRandomKey()
		}

		if !ok {
			break
		}

		if info, ok := db.GetKeyInfo(key); ok {
			if db.DeleteSingle(key) {
				evicted++
				freed += info.Size
				if freed < 64 {
					freed = 64
				}
			}
		}
	}

	return evicted, freed
}

// TTLPolicy implements eviction based on shortest TTL
type TTLPolicy struct {
	basePolicy
	pool *EvictionPool
}

func NewTTLPolicy() *TTLPolicy {
	return &TTLPolicy{
		basePolicy: basePolicy{policyType: PolicyVolatileTTL},
		pool:       NewEvictionPool(16),
	}
}

func (p *TTLPolicy) Evict(db DBAccessor, samples int, bytesNeeded int64) (int, int64) {
	evicted := 0
	var freed int64

	// Fill the eviction pool with keys having expiration
	p.FillPool(db, samples)

	// Evict from the pool (shortest TTL first)
	for evicted < 32 && freed < bytesNeeded {
		keyInfo := p.pool.PopBest()
		if keyInfo == nil {
			break
		}

		if db.DeleteSingle(keyInfo.Key) {
			evicted++
			freed += keyInfo.Size
			if freed < 64 {
				freed = 64
			}
		}
	}

	return evicted, freed
}

// FillPool fills the eviction pool with keys that have expiration
func (p *TTLPolicy) FillPool(db DBAccessor, samples int) {
	count := db.GetKeysWithExpirationCount()
	if count == 0 {
		return
	}

	if samples > count {
		samples = count
	}

	now := time.Now().Unix()

	for i := 0; i < samples; i++ {
		key, ok := db.GetRandomKeyWithExpiration()
		if !ok {
			continue
		}

		info, ok := db.GetKeyInfo(key)
		if !ok || info.ExpiresAt == 0 {
			continue
		}

		// Calculate TTL (shorter is better for eviction)
		ttl := info.ExpiresAt - now
		if ttl < 0 {
			ttl = 0
		}

		// Use idle time as secondary factor
		idle := uint32(0)
		if info.LRU > 0 {
			idle = uint32(now) - info.LRU
		}

		// Primary sort: TTL, secondary sort: idle time
		// Lower TTL gets higher score (we store idle for secondary)
		p.pool.InsertWithTTL(key, uint32(ttl), idle, info.Size)
	}
}

// NewPolicy creates a policy from a PolicyType
func NewPolicy(policyType PolicyType) Policy {
	switch policyType {
	case PolicyAllKeysLRU:
		return NewLRUPolicy(false)
	case PolicyVolatileLRU:
		return NewLRUPolicy(true)
	case PolicyAllKeysLFU:
		return NewLFUPolicy(false)
	case PolicyVolatileLFU:
		return NewLFUPolicy(true)
	case PolicyAllKeysRandom:
		return NewRandomPolicy(false)
	case PolicyVolatileRandom:
		return NewRandomPolicy(true)
	case PolicyVolatileTTL:
		return NewTTLPolicy()
	default:
		return NewNoEvictionPolicy()
	}
}

// EvictionPool manages a pool of eviction candidates
type EvictionPool struct {
	sync.Mutex
	buckets [][]*PoolEntry
	size    int
}

// PoolEntry represents an entry in the eviction pool
type PoolEntry struct {
	Key       string
	Score     uint32 // Lower is better for eviction
	Size      int64
	ExpiresAt int64
	LastUsed  uint32
}

// NewEvictionPool creates a new eviction pool
func NewEvictionPool(size int) *EvictionPool {
	return &EvictionPool{
		buckets: make([][]*PoolEntry, 256),
		size:    size,
	}
}

// Insert adds a key to the eviction pool
func (p *EvictionPool) Insert(key string, idle uint32, size int64, expiresAt int64) {
	p.Lock()
	defer p.Unlock()

	// Calculate bucket based on idle time
	bucketIdx := int(idle % 256)

	// Check if key already exists in pool
	for _, entry := range p.buckets[bucketIdx] {
		if entry.Key == key {
			entry.Score = idle
			entry.Size = size
			entry.ExpiresAt = expiresAt
			return
		}
	}

	// Add new entry
	entry := &PoolEntry{
		Key:       key,
		Score:     idle,
		Size:      size,
		ExpiresAt: expiresAt,
	}

	p.buckets[bucketIdx] = append(p.buckets[bucketIdx], entry)

	// Keep bucket size under control
	if len(p.buckets[bucketIdx]) > p.size {
		// Remove oldest entries (highest score)
		p.evictOldestFromBucket(bucketIdx)
	}
}

// InsertWithTTL adds a key to the eviction pool with TTL as primary factor
func (p *EvictionPool) InsertWithTTL(key string, ttl, idle uint32, size int64) {
	p.Lock()
	defer p.Unlock()

	// For TTL, use lower buckets for shorter TTL
	// TTL 0-255 goes to bucket 0, etc.
	bucketIdx := int(ttl % 256)

	// Check if key already exists
	for _, entry := range p.buckets[bucketIdx] {
		if entry.Key == key {
			entry.Score = ttl
			entry.Size = size
			entry.LastUsed = idle
			return
		}
	}

	// Add new entry
	entry := &PoolEntry{
		Key:      key,
		Score:    ttl,
		Size:     size,
		LastUsed: idle,
	}

	p.buckets[bucketIdx] = append(p.buckets[bucketIdx], entry)

	if len(p.buckets[bucketIdx]) > p.size {
		p.evictOldestFromBucket(bucketIdx)
	}
}

// PopBest returns the best eviction candidate
func (p *EvictionPool) PopBest() *PoolEntry {
	p.Lock()
	defer p.Unlock()

	// Search buckets in order (lower idle/TTL first)
	for i := range p.buckets {
		if len(p.buckets[i]) == 0 {
			continue
		}

		// Find best entry in this bucket (highest score = least recently used)
		bestIdx := 0
		bestScore := p.buckets[i][0].Score

		for j, entry := range p.buckets[i] {
			if entry.Score > bestScore {
				bestScore = entry.Score
				bestIdx = j
			}
		}

		// Remove and return best entry
		entry := p.buckets[i][bestIdx]
		p.buckets[i] = append(p.buckets[i][:bestIdx], p.buckets[i][bestIdx+1:]...)
		return entry
	}

	return nil
}

// evictOldestFromBucket removes entries with highest score from a bucket
func (p *EvictionPool) evictOldestFromBucket(bucket int) {
	if len(p.buckets[bucket]) <= p.size {
		return
	}

	// Sort by score descending and remove excess
	// For simplicity, just remove from end
	p.buckets[bucket] = p.buckets[bucket][:p.size]
}

// Size returns the total number of entries in the pool
func (p *EvictionPool) Size() int {
	p.Lock()
	defer p.Unlock()

	total := 0
	for _, bucket := range p.buckets {
		total += len(bucket)
	}
	return total
}

// Clear removes all entries from the pool
func (p *EvictionPool) Clear() {
	p.Lock()
	defer p.Unlock()

	p.buckets = make([][]*PoolEntry, 256)
}
