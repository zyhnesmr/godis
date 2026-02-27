// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package database

import (
	"fmt"
	"sync"
	"time"

	"github.com/zyhnesmr/godis/internal/eviction"
)

// DirtyKeyCallback is called when a key is modified
type DirtyKeyCallback func(key string)

// DB represents a single Redis database
type DB struct {
	id      int
	dict    *Dict
	expires *Dict
	mu      sync.RWMutex

	// Statistics
	keysCount int64

	// Transaction support
	dirtyKeyCallback DirtyKeyCallback
}

// NewDB creates a new database
func NewDB(id int) *DB {
	return &DB{
		id:        id,
		dict:      NewDict(),
		expires:   NewDict(),
		keysCount: 0,
	}
}

// SetDirtyKeyCallback sets the callback for marking dirty keys
func (db *DB) SetDirtyKeyCallback(cb DirtyKeyCallback) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.dirtyKeyCallback = cb
}

// markDirty marks a key as dirty (modified)
func (db *DB) markDirty(key string) {
	if db.dirtyKeyCallback != nil {
		db.dirtyKeyCallback(key)
	}
}

// GetID returns the database ID
func (db *DB) GetID() int {
	return db.id
}

// Get returns the value for a key, with lazy expiration on access
func (db *DB) Get(key string) (*Object, bool) {
	// First try with read lock
	db.mu.RLock()
	obj, ok := db.dict.Get(key)
	if !ok {
		db.mu.RUnlock()
		return nil, false
	}

	// Check if expired
	expired := db.isExpiredLocked(key)
	if !expired {
		defer db.mu.RUnlock()
		return obj.(*Object), true
	}

	// Key is expired, upgrade to write lock for lazy deletion
	db.mu.RUnlock()
	db.mu.Lock()

	// Double-check after acquiring write lock
	obj, ok = db.dict.Get(key)
	if ok && db.isExpiredLocked(key) {
		// Lazy delete the expired key
		db.dict.Delete(key)
		db.expires.Delete(key)
		db.keysCount--
		db.mu.Unlock()
		return nil, false
	}

	// Key was refreshed or deleted by another goroutine
	defer db.mu.Unlock()
	if ok {
		return obj.(*Object), true
	}
	return nil, false
}

// Set sets a key-value pair
func (db *DB) Set(key string, value *Object) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if key exists and is not expired
	wasNew := !db.dict.Exists(key) || db.isExpiredLocked(key)
	db.dict.Set(key, value)

	if wasNew {
		db.keysCount++
	}

	db.markDirty(key)
}

// SetNX sets a key-value pair only if key doesn't exist
func (db *DB) SetNX(key string, value *Object) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.dict.Exists(key) && !db.isExpiredLocked(key) {
		return false
	}

	db.dict.Set(key, value)
	db.keysCount++
	db.markDirty(key)
	return true
}

// SetXX sets a key-value pair only if key exists
func (db *DB) SetXX(key string, value *Object) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.dict.Exists(key) || db.isExpiredLocked(key) {
		return false
	}

	db.dict.Set(key, value)
	db.markDirty(key)
	return true
}

// Delete removes keys from the database
func (db *DB) Delete(keys ...string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	deleted := 0
	for _, key := range keys {
		if db.dict.Exists(key) {
			db.dict.Delete(key)
			db.expires.Delete(key)
			db.keysCount--
			deleted++
			db.markDirty(key)
		}
	}

	return deleted
}

// Exists checks if keys exist
func (db *DB) Exists(keys ...string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	count := 0
	for _, key := range keys {
		if db.dict.Exists(key) && !db.isExpiredLocked(key) {
			count++
		}
	}

	return count
}

// Type returns the type of a key
func (db *DB) Type(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if obj, ok := db.dict.Get(key); ok && !db.isExpiredLocked(key) {
		if o, ok := obj.(*Object); ok {
			return o.Type.String()
		}
	}

	return "none"
}

// Keys returns all keys matching a pattern
func (db *DB) Keys(pattern string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Simple pattern matching (only * supported for now)
	allKeys := db.dict.Keys()

	if pattern == "*" {
		return allKeys
	}

	// Filter by pattern
	result := make([]string, 0)
	for _, key := range allKeys {
		if !db.isExpiredLocked(key) && matchPattern(key, pattern) {
			result = append(result, key)
		}
	}

	return result
}

// RandomKey returns a random key
func (db *DB) RandomKey() (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := 0; i < 100; i++ {
		key, ok := db.dict.RandomKey()
		if !ok {
			return "", false
		}
		if !db.isExpiredLocked(key) {
			return key, true
		}
	}

	return "", false
}

// Rename renames a key
func (db *DB) Rename(key, newKey string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if key == newKey {
		return nil
	}

	obj, ok := db.dict.Get(key)
	if !ok || db.isExpiredLocked(key) {
		return fmt.Errorf("no such key")
	}

	// Get expire time
	var expireTime int64
	if exp, ok := db.expires.Get(key); ok {
		expireTime = exp.(int64)
	}

	// Delete old keys
	db.dict.Delete(key)
	db.expires.Delete(key)

	// Set new key
	db.dict.Set(newKey, obj)
	if expireTime > 0 {
		db.expires.Set(newKey, expireTime)
	}

	db.markDirty(key)
	db.markDirty(newKey)
	return nil
}

// RenameNX renames a key only if new key doesn't exist
func (db *DB) RenameNX(key, newKey string) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if key == newKey {
		return false, nil
	}

	obj, ok := db.dict.Get(key)
	if !ok || db.isExpiredLocked(key) {
		return false, fmt.Errorf("no such key")
	}

	if db.dict.Exists(newKey) && !db.isExpiredLocked(newKey) {
		return false, nil
	}

	// Get expire time
	var expireTime int64
	if exp, ok := db.expires.Get(key); ok {
		expireTime = exp.(int64)
	}

	// Delete old keys
	db.dict.Delete(key)
	db.expires.Delete(key)

	// Set new key
	db.dict.Set(newKey, obj)
	if expireTime > 0 {
		db.expires.Set(newKey, expireTime)
	}

	db.markDirty(key)
	db.markDirty(newKey)
	return true, nil
}

// Expire sets an expiration time for a key (in seconds)
func (db *DB) Expire(key string, seconds int) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.dict.Exists(key) {
		return false
	}

	expireTime := time.Now().Add(time.Duration(seconds) * time.Second).Unix()
	db.expires.Set(key, expireTime)
	return true
}

// ExpireAt sets an expiration timestamp for a key
func (db *DB) ExpireAt(key string, timestamp int64) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.dict.Exists(key) {
		return false
	}

	db.expires.Set(key, timestamp)
	return true
}

// TTL returns the time to live for a key (in seconds)
func (db *DB) TTL(key string) int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.dict.Exists(key) {
		return -2 // Key doesn't exist
	}

	exp, ok := db.expires.Get(key)
	if !ok {
		return -1 // No expiration
	}

	ttl := exp.(int64) - time.Now().Unix()
	if ttl <= 0 {
		return -2 // Already expired
	}

	return ttl
}

// PTTL returns the time to live for a key (in milliseconds)
func (db *DB) PTTL(key string) int64 {
	return db.TTL(key) * 1000
}

// Persist removes the expiration from a key
func (db *DB) Persist(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.dict.Exists(key) {
		return false
	}

	_, ok := db.expires.Get(key)
	if !ok {
		return false
	}

	db.expires.Delete(key)
	return true
}

// DBSize returns the number of keys in the database
func (db *DB) DBSize() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Count non-expired keys
	count := 0
	for _, key := range db.dict.Keys() {
		if !db.isExpiredLocked(key) {
			count++
		}
	}

	return count
}

// FlushDB removes all keys from the database
func (db *DB) FlushDB() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.dict.Clear()
	db.expires.Clear()
	db.keysCount = 0
}

// isExpired checks if a key is expired (without holding lock)
func (db *DB) isExpired(key string) bool {
	exp, ok := db.expires.Get(key)
	if !ok {
		return false
	}

	return exp.(int64) <= time.Now().Unix()
}

// isExpiredLocked checks if a key is expired (with lock held)
func (db *DB) isExpiredLocked(key string) bool {
	exp, ok := db.expires.Get(key)
	if !ok {
		return false
	}

	return exp.(int64) <= time.Now().Unix()
}

// matchPattern checks if a key matches a pattern
func matchPattern(key, pattern string) bool {
	// Simple glob matching
	if pattern == "*" {
		return true
	}

	// Handle %*% pattern (contains)
	if len(pattern) > 1 && pattern[0] == '*' && pattern[len(pattern)-1] == '*' {
		sub := pattern[1 : len(pattern)-1]
		return contains(key, sub)
	}

	// Handle %* prefix pattern
	if pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	}

	// Handle %* suffix pattern
	if pattern[0] == '*' {
		suffix := pattern[1:]
		return len(key) >= len(suffix) && key[len(key)-len(suffix):] == suffix
	}

	return key == pattern
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && findContains(s, substr)
}

func findContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ActiveExpire actively removes expired keys
func (db *DB) ActiveExpire(limit int) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	expired := 0
	now := time.Now().Unix()

	// Get all keys
	allKeys := db.expires.Keys()

	for _, key := range allKeys {
		if expired >= limit {
			break
		}

		exp, ok := db.expires.Get(key)
		if ok && exp.(int64) <= now {
			db.dict.Delete(key)
			db.expires.Delete(key)
			db.keysCount--
			expired++
			db.markDirty(key)
		}
	}

	return expired
}

// GetExpiresDict returns the expires dictionary
func (db *DB) GetExpiresDict() *Dict {
	return db.expires
}

// GetDict returns the main dictionary
func (db *DB) GetDict() *Dict {
	return db.dict
}

// Scan scans keys with cursor
func (db *DB) Scan(cursor int, count int, pattern string) (int, []string) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	keys := db.dict.Keys()

	result := make([]string, 0)

	start := cursor
	end := cursor + count
	if end > len(keys) {
		end = len(keys)
	}

	for i := start; i < end; i++ {
		key := keys[i]
		if !db.isExpiredLocked(key) && matchPattern(key, pattern) {
			result = append(result, key)
		}
	}

	if end >= len(keys) {
		return 0, result
	}

	return end, result
}

// Stats returns database statistics
func (db *DB) Stats() DBStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return DBStats{
		ID:      db.id,
		Keys:    db.keysCount,
		Expires: db.expires.Len(),
	}
}

// DBStats holds database statistics
type DBStats struct {
	ID      int
	Keys    int64
	Expires int
}

// ==================== Eviction Support ====================

// GetKeyInfo returns information about a key for eviction decisions
func (db *DB) GetKeyInfo(key string) (*eviction.KeyInfo, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	obj, ok := db.dict.Get(key)
	if !ok || db.isExpiredLocked(key) {
		return nil, false
	}

	object, ok := obj.(*Object)
	if !ok {
		return nil, false
	}

	var expiresAt int64
	if exp, ok := db.expires.Get(key); ok {
		expiresAt = exp.(int64)
	}

	return &eviction.KeyInfo{
		Key:       key,
		LRU:       object.LRU,
		ExpiresAt: expiresAt,
		Size:      object.Size(),
	}, true
}

// GetRandomKey returns a random key from the database
func (db *DB) GetRandomKey() (string, bool) {
	return db.RandomKey()
}

// GetRandomKeyWithExpiration returns a random key that has an expiration
func (db *DB) GetRandomKeyWithExpiration() (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Try up to 100 times to find a key with expiration
	for i := 0; i < 100; i++ {
		key, ok := db.dict.RandomKey()
		if !ok {
			return "", false
		}

		if !db.isExpiredLocked(key) {
			if _, hasExp := db.expires.Get(key); hasExp {
				return key, true
			}
		}
	}

	return "", false
}

// GetKeysCount returns the total number of keys in the database
func (db *DB) GetKeysCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Count non-expired keys
	count := 0
	for _, key := range db.dict.Keys() {
		if !db.isExpiredLocked(key) {
			count++
		}
	}
	return count
}

// GetKeysWithExpirationCount returns the number of keys with expiration
func (db *DB) GetKeysWithExpirationCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	count := 0
	for _, key := range db.expires.Keys() {
		if db.dict.Exists(key) && !db.isExpiredLocked(key) {
			count++
		}
	}
	return count
}

// Delete removes a key from the database (for eviction)
// This implements eviction.DBAccessor interface
func (db *DB) DeleteForEviction(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.dict.Exists(key) {
		return false
	}

	db.dict.Delete(key)
	db.expires.Delete(key)
	db.keysCount--
	return true
}

// DeleteSingle removes a single key from the database (implements eviction.DBAccessor.Delete)
func (db *DB) DeleteSingle(key string) bool {
	return db.DeleteForEviction(key)
}

// GetMemoryUsage returns the approximate memory usage of the database
func (db *DB) GetMemoryUsage() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var total int64

	// Estimate memory from all objects
	for _, key := range db.dict.Keys() {
		if db.isExpiredLocked(key) {
			continue
		}

		if obj, ok := db.dict.Get(key); ok {
			if o, ok := obj.(*Object); ok {
				total += o.Size()
				total += int64(len(key)) // Add key size
			}
		}
	}

	// Add overhead for dictionaries
	total += int64(db.dict.Len()) * 16 // Approximate pointer overhead

	return total
}
