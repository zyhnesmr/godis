// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package database

import (
	"fmt"
	"sync"

	"github.com/zyhnesmr/godis/internal/eviction"
)

// DBSelector manages multiple databases
type DBSelector struct {
	dbs      []*DB
	mu       sync.RWMutex
	count    int

	// Eviction management
	evictionMgr *eviction.Manager
	maxMemory   int64
}

// NewDBSelector creates a new database selector
func NewDBSelector(count int) *DBSelector {
	if count <= 0 {
		count = 16 // Default to 16 databases
	}

	dbs := make([]*DB, count)
	for i := 0; i < count; i++ {
		dbs[i] = NewDB(i)
	}

	s := &DBSelector{
		dbs:    dbs,
		count:  count,
	}

	// Initialize eviction manager
	s.evictionMgr = eviction.NewManager(eviction.PolicyNoEviction, 0, 5)

	return s
}

// NewDBSelectorWithEviction creates a new database selector with eviction policy
func NewDBSelectorWithEviction(count int, policyType eviction.PolicyType, maxMemory int64) *DBSelector {
	if count <= 0 {
		count = 16
	}

	dbs := make([]*DB, count)
	for i := 0; i < count; i++ {
		dbs[i] = NewDB(i)
	}

	s := &DBSelector{
		dbs:       dbs,
		count:     count,
		maxMemory: maxMemory,
	}

	s.evictionMgr = eviction.NewManager(policyType, maxMemory, 5)

	return s
}

// GetDB returns a database by index
func (s *DBSelector) GetDB(index int) (*DB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < 0 || index >= s.count {
		return nil, fmt.Errorf("DB index out of range: %d", index)
	}

	return s.dbs[index], nil
}

// GetDBWithoutLock returns a database without locking (use with caution)
func (s *DBSelector) GetDBWithoutLock(index int) (*DB, error) {
	if index < 0 || index >= s.count {
		return nil, fmt.Errorf("DB index out of range: %d", index)
	}

	return s.dbs[index], nil
}

// GetDefaultDB returns the default database (index 0)
func (s *DBSelector) GetDefaultDB() *DB {
	return s.dbs[0]
}

// Count returns the number of databases
func (s *DBSelector) Count() int {
	return s.count
}

// FlushAll flushes all databases
func (s *DBSelector) FlushAll() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, db := range s.dbs {
		db.FlushDB()
	}
}

// TotalKeys returns the total number of keys across all databases
func (s *DBSelector) TotalKeys() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := 0
	for _, db := range s.dbs {
		total += db.DBSize()
	}

	return total
}

// Stats returns statistics for all databases
func (s *DBSelector) Stats() []DBStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make([]DBStats, s.count)
	for i, db := range s.dbs {
		stats[i] = db.Stats()
	}

	return stats
}

// ActiveExpireAll actively expires keys across all databases
func (s *DBSelector) ActiveExpireAll(limitPerDB int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalExpired := 0
	for _, db := range s.dbs {
		totalExpired += db.ActiveExpire(limitPerDB)
	}

	return totalExpired
}

// ==================== Eviction Management ====================

// GetEvictionManager returns the eviction manager
func (s *DBSelector) GetEvictionManager() *eviction.Manager {
	return s.evictionMgr
}

// SetEvictionPolicy sets the eviction policy
func (s *DBSelector) SetEvictionPolicy(policyType eviction.PolicyType) {
	s.evictionMgr.SetPolicy(policyType)
}

// GetEvictionPolicy returns the current eviction policy
func (s *DBSelector) GetEvictionPolicy() eviction.PolicyType {
	return s.evictionMgr.GetPolicy()
}

// SetMaxMemory sets the maximum memory limit
func (s *DBSelector) SetMaxMemory(maxMemory int64) {
	s.mu.Lock()
	s.maxMemory = maxMemory
	s.mu.Unlock()

	s.evictionMgr.SetMaxMemory(maxMemory)
}

// GetMaxMemory returns the maximum memory limit
func (s *DBSelector) GetMaxMemory() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxMemory
}

// GetTotalMemoryUsage returns the total memory usage across all databases
func (s *DBSelector) GetTotalMemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total int64
	for _, db := range s.dbs {
		total += db.GetMemoryUsage()
	}
	return total
}

// ShouldEvict checks if eviction should be performed
func (s *DBSelector) ShouldEvict() bool {
	return s.evictionMgr.ShouldEvict()
}

// ProcessEviction attempts to evict keys to free up memory
func (s *DBSelector) ProcessEviction(bytesNeeded int64) (int, error) {
	// Collect all databases as DBAccessor
	dbs := make([]eviction.DBAccessor, len(s.dbs))
	s.mu.RLock()
	for i, db := range s.dbs {
		dbs[i] = db
	}
	s.mu.RUnlock()

	return s.evictionMgr.ProcessEvictionForDBs(dbs, bytesNeeded)
}

// CheckAndEvict checks if eviction is needed and performs it
func (s *DBSelector) CheckAndEvict() error {
	if !s.ShouldEvict() {
		return nil
	}

	_, err := s.ProcessEviction(0)
	return err
}

// GetEvictionStats returns eviction statistics
func (s *DBSelector) GetEvictionStats() eviction.Stats {
	return s.evictionMgr.GetStats()
}
