// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package database

import (
	"fmt"
	"sync"
)

// DBSelector manages multiple databases
type DBSelector struct {
	dbs   []*DB
	mu    sync.RWMutex
	count int
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

	return &DBSelector{
		dbs:   dbs,
		count: count,
	}
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
