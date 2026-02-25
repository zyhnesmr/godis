// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package eviction

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Stats holds eviction statistics
type Stats struct {
	Policy              PolicyType
	CurrentMemory       int64
	MaxMemory           int64
	KeysEvicted         int64
	BytesFreed          int64
	LastEvictionTime    time.Time
	EvictionCycles      int64
	OOMCount            int64
	PoolSize            int
}

// Manager manages eviction for multiple databases
type Manager struct {
	sync.RWMutex

	policy      Policy
	policyType  PolicyType
	maxMemory   int64
	enabled     bool

	// Statistics
	keysEvicted  int64
	bytesFreed   int64
	evictionCycles int64
	oomCount     int64
	lastEvictionTime time.Time

	// Configuration
	samples      int

	// Callback to get current memory usage
	getMemoryUsage func() int64
}

// NewManager creates a new eviction manager
func NewManager(policyType PolicyType, maxMemory int64, samples int) *Manager {
	m := &Manager{
		policyType: policyType,
		policy:     NewPolicy(policyType),
		maxMemory:  maxMemory,
		enabled:    maxMemory > 0 && policyType != PolicyNoEviction,
		samples:    samples,
	}

	if m.samples <= 0 {
		m.samples = 5 // Default samples
	}

	return m
}

// SetMemoryUsageCallback sets the callback to get current memory usage
func (m *Manager) SetMemoryUsageCallback(fn func() int64) {
	m.Lock()
	defer m.Unlock()
	m.getMemoryUsage = fn
}

// GetCurrentMemory returns the current memory usage
func (m *Manager) GetCurrentMemory() int64 {
	if m.getMemoryUsage != nil {
		return m.getMemoryUsage()
	}
	return 0
}

// ShouldEvict checks if eviction should be performed
func (m *Manager) ShouldEvict() bool {
	m.RLock()
	defer m.RUnlock()

	if !m.enabled {
		return false
	}

	currentMemory := m.GetCurrentMemory()
	return m.maxMemory > 0 && currentMemory >= m.maxMemory
}

// ProcessEviction attempts to evict keys to free up memory
// Returns the number of keys evicted and an error if failed
func (m *Manager) ProcessEviction(db DBAccessor, bytesNeeded int64) (int, error) {
	m.Lock()
	defer m.Unlock()

	if !m.enabled || m.policy == nil {
		return 0, fmt.Errorf("eviction is disabled")
	}

	currentMemory := m.GetCurrentMemory()
	if m.maxMemory == 0 || currentMemory < m.maxMemory {
		return 0, nil
	}

	// Calculate how much memory we need to free
	if bytesNeeded <= 0 {
		bytesNeeded = currentMemory - m.maxMemory + 1
		// Free at least 5% of max memory to avoid frequent evictions
		fivePercent := m.maxMemory / 20
		if bytesNeeded < fivePercent {
			bytesNeeded = fivePercent
		}
	}

	// Perform eviction
	evicted, freed := m.policy.Evict(db, m.samples, bytesNeeded)

	atomic.AddInt64(&m.keysEvicted, int64(evicted))
	atomic.AddInt64(&m.bytesFreed, freed)
	atomic.AddInt64(&m.evictionCycles, 1)
	m.lastEvictionTime = time.Now()

	// Check if we're still over limit
	if m.ShouldEvictAfterEvict() {
		atomic.AddInt64(&m.oomCount, 1)
	}

	return evicted, nil
}

// ShouldEvictAfterEvict checks if eviction is still needed after eviction cycle
func (m *Manager) ShouldEvictAfterEvict() bool {
	currentMemory := m.GetCurrentMemory()
	return m.maxMemory > 0 && currentMemory >= m.maxMemory
}

// ProcessEvictionForDBs attempts to evict keys from multiple databases
func (m *Manager) ProcessEvictionForDBs(dbs []DBAccessor, bytesNeeded int64) (int, error) {
	totalEvicted := 0

	for _, db := range dbs {
		evicted, err := m.ProcessEviction(db, bytesNeeded)
		if err != nil {
			return totalEvicted, err
		}

		totalEvicted += evicted

		// Stop if we've freed enough memory
		if !m.ShouldEvict() {
			break
		}
	}

	return totalEvicted, nil
}

// SetPolicy changes the eviction policy
func (m *Manager) SetPolicy(policyType PolicyType) {
	m.Lock()
	defer m.Unlock()

	m.policyType = policyType
	m.policy = NewPolicy(policyType)
	m.enabled = m.maxMemory > 0 && policyType != PolicyNoEviction
}

// GetPolicy returns the current policy type
func (m *Manager) GetPolicy() PolicyType {
	m.RLock()
	defer m.RUnlock()
	return m.policyType
}

// SetMaxMemory sets the maximum memory limit
func (m *Manager) SetMaxMemory(maxMemory int64) {
	m.Lock()
	defer m.Unlock()

	m.maxMemory = maxMemory
	m.enabled = maxMemory > 0 && m.policyType != PolicyNoEviction
}

// GetMaxMemory returns the maximum memory limit
func (m *Manager) GetMaxMemory() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.maxMemory
}

// Enable enables or disables eviction
func (m *Manager) Enable(enabled bool) {
	m.Lock()
	defer m.Unlock()
	m.enabled = enabled && m.maxMemory > 0 && m.policyType != PolicyNoEviction
}

// IsEnabled returns whether eviction is enabled
func (m *Manager) IsEnabled() bool {
	m.RLock()
	defer m.RUnlock()
	return m.enabled
}

// GetStats returns eviction statistics
func (m *Manager) GetStats() Stats {
	m.RLock()
	defer m.RUnlock()

	return Stats{
		Policy:           m.policyType,
		CurrentMemory:    m.GetCurrentMemory(),
		MaxMemory:        m.maxMemory,
		KeysEvicted:      atomic.LoadInt64(&m.keysEvicted),
		BytesFreed:       atomic.LoadInt64(&m.bytesFreed),
		LastEvictionTime: m.lastEvictionTime,
		EvictionCycles:   atomic.LoadInt64(&m.evictionCycles),
		OOMCount:         atomic.LoadInt64(&m.oomCount),
	}
}

// ResetStats resets eviction statistics
func (m *Manager) ResetStats() {
	m.Lock()
	defer m.Unlock()

	atomic.StoreInt64(&m.keysEvicted, 0)
	atomic.StoreInt64(&m.bytesFreed, 0)
	atomic.StoreInt64(&m.evictionCycles, 0)
	atomic.StoreInt64(&m.oomCount, 0)
	m.lastEvictionTime = time.Time{}
}

// MemoryUsagePercent returns the current memory usage as a percentage
func (m *Manager) MemoryUsagePercent() float64 {
	m.RLock()
	defer m.RUnlock()

	if m.maxMemory == 0 {
		return 0
	}

	currentMemory := m.GetCurrentMemory()
	return float64(currentMemory) / float64(m.maxMemory) * 100
}

// IsNearMemoryLimit returns true if memory usage is above the threshold
func (m *Manager) IsNearMemoryLimit(thresholdPercent float64) bool {
	m.RLock()
	defer m.RUnlock()

	if m.maxMemory == 0 {
		return false
	}

	currentMemory := m.GetCurrentMemory()
	usagePercent := float64(currentMemory) / float64(m.maxMemory) * 100
	return usagePercent >= thresholdPercent
}

// GetPolicyName returns the name of the current policy
func (m *Manager) GetPolicyName() string {
	m.RLock()
	defer m.RUnlock()
	return m.policyType.String()
}
