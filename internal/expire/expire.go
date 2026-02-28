// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package expire

import (
	"sync"
	"time"
)

// ExpireKeyType represents the type of key in expire entries
type ExpireKeyType struct {
	DB  int
	Key string
}

// ExpireEntry represents an entry in the expire set
type ExpireEntry struct {
	Key      ExpireKeyType
	Deadline int64 // Unix timestamp in seconds
}

// Manager manages key expiration for all databases
type Manager struct {
	sync.RWMutex

	// Active expiration config
	enabled          bool
	activeExpireRuns int

	// Time wheel for efficient expiration
	timeWheel *TimeWheel

	// Map for tracking all keys with expiration
	// key: db_index-key, value: deadline
	expireMap map[string]int64

	// Callbacks for database operations
	onExpireFunc func(db int, key string)

	// Statistics
	expiredCount int64
	checkCount   int64
}

// NewManager creates a new expire manager
func NewManager(onExpire func(db int, key string)) *Manager {
	return &Manager{
		enabled:      true,
		timeWheel:    NewTimeWheel(10, 512), // 10ms tick, 512 slots
		expireMap:    make(map[string]int64),
		onExpireFunc: onExpire,
	}
}

// Enable enables or disables the expire manager
func (m *Manager) Enable(enabled bool) {
	m.Lock()
	defer m.Unlock()
	m.enabled = enabled
}

// Enabled returns whether the expire manager is enabled
func (m *Manager) Enabled() bool {
	m.RLock()
	defer m.RUnlock()
	return m.enabled
}

// Add adds a key with expiration time
func (m *Manager) Add(db int, key string, deadline int64) {
	m.Lock()
	defer m.Unlock()

	if !m.enabled {
		return
	}

	// Create composite key
	compositeKey := m.makeKey(db, key)

	// Update expire map
	m.expireMap[compositeKey] = deadline

	// Add to time wheel
	deadlineTime := time.Unix(deadline, 0)
	m.timeWheel.Add(compositeKey, deadlineTime)
}

// Remove removes a key from expiration tracking
func (m *Manager) Remove(db int, key string) {
	m.Lock()
	defer m.Unlock()

	compositeKey := m.makeKey(db, key)
	delete(m.expireMap, compositeKey)
	m.timeWheel.Remove(compositeKey)
}

// Get returns the expiration time for a key
func (m *Manager) Get(db int, key string) (int64, bool) {
	m.RLock()
	defer m.RUnlock()

	compositeKey := m.makeKey(db, key)
	deadline, ok := m.expireMap[compositeKey]
	return deadline, ok
}

// IsExpired checks if a key is expired
func (m *Manager) IsExpired(db int, key string) bool {
	m.RLock()
	defer m.RUnlock()

	compositeKey := m.makeKey(db, key)
	deadline, ok := m.expireMap[compositeKey]
	if !ok {
		return false
	}

	return deadline <= time.Now().Unix()
}

// Tick advances the time wheel and processes expired keys
func (m *Manager) Tick() []ExpireEntry {
	m.Lock()
	defer m.Unlock()

	if !m.enabled {
		return nil
	}

	expiredKeys := m.timeWheel.Advance()
	if len(expiredKeys) == 0 {
		return nil
	}

	now := time.Now().Unix()
	entries := make([]ExpireEntry, 0, len(expiredKeys))

	for _, compositeKey := range expiredKeys {
		deadline, ok := m.expireMap[compositeKey]
		if !ok {
			continue
		}

		// Verify it's actually expired
		if deadline <= now {
			db, key := m.parseKey(compositeKey)
			entries = append(entries, ExpireEntry{
				Key:      ExpireKeyType{DB: db, Key: key},
				Deadline: deadline,
			})

			// Remove from tracking
			delete(m.expireMap, compositeKey)
		}
	}

	return entries
}

// ActiveExpire performs active expiration scanning
// Similar to Redis's activeExpireCycle
func (m *Manager) ActiveExpire(databases []ActiveExpireDB) int {
	m.Lock()
	defer m.Unlock()

	if !m.enabled {
		return 0
	}

	m.activeExpireRuns++

	// Each run, we try to expire some keys
	// Limit the work we do per cycle
	effort := 20 // Default effort: check 20 keys
	totalExpired := 0

	for _, db := range databases {
		if effort <= 0 {
			break
		}

		expired := db.ScanExpire(effort)
		totalExpired += expired
		effort -= expired
	}

	m.expiredCount += int64(totalExpired)
	return totalExpired
}

// ProcessExpired processes expired entries using the callback
func (m *Manager) ProcessExpired(entries []ExpireEntry) {
	if m.onExpireFunc == nil {
		return
	}

	for _, entry := range entries {
		m.onExpireFunc(entry.Key.DB, entry.Key.Key)
	}
}

// Size returns the number of keys being tracked
func (m *Manager) Size() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.expireMap)
}

// Stats returns expire statistics
func (m *Manager) Stats() ExpireStats {
	m.RLock()
	defer m.RUnlock()

	return ExpireStats{
		Enabled:          m.enabled,
		TrackedKeys:      len(m.expireMap),
		ActiveExpireRuns: m.activeExpireRuns,
		ExpiredCount:     m.expiredCount,
		CheckCount:       m.checkCount,
	}
}

// Stop stops the expire manager
func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()

	m.enabled = false
	m.timeWheel.Stop()
}

// makeKey creates a composite key for tracking
func (m *Manager) makeKey(db int, key string) string {
	return string(rune(db)) + ":" + key
}

// parseKey parses a composite key
func (m *Manager) parseKey(compositeKey string) (int, string) {
	// Simple parsing: format is "db:key"
	for i := 0; i < len(compositeKey); i++ {
		if compositeKey[i] == ':' {
			db := int(compositeKey[0])
			key := compositeKey[i+1:]
			return db, key
		}
	}
	return 0, compositeKey
}

// ExpireStats holds expire statistics
type ExpireStats struct {
	Enabled          bool
	TrackedKeys      int
	ActiveExpireRuns int
	ExpiredCount     int64
	CheckCount       int64
}

// ActiveExpireDB represents a database interface for active expiration
type ActiveExpireDB interface {
	// ScanExpire scans and expires up to N keys, returns number expired
	ScanExpire(n int) int
}
