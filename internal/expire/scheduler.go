// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package expire

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zyhnesmr/godis/pkg/log"
)

// Scheduler manages background expire tasks
type Scheduler struct {
	mgr *Manager

	// Scheduling control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool

	// Configuration
	config Config
}

// Config holds scheduler configuration
type Config struct {
	// TickInterval is how often to advance the time wheel
	TickInterval time.Duration

	// ActiveExpireInterval is how often to run active expiration
	ActiveExpireInterval time.Duration

	// FastCycleInterval is how often to run fast expiration cycle
	FastCycleInterval time.Duration
}

// DefaultConfig returns default scheduler configuration
func DefaultConfig() Config {
	return Config{
		TickInterval:         10 * time.Millisecond,
		ActiveExpireInterval: 100 * time.Millisecond,
		FastCycleInterval:    10 * time.Millisecond,
	}
}

// NewScheduler creates a new expire scheduler
func NewScheduler(mgr *Manager) *Scheduler {
	return &Scheduler{
		mgr:    mgr,
		config: DefaultConfig(),
	}
}

// SetConfig sets the scheduler configuration
func (s *Scheduler) SetConfig(config Config) {
	s.config = config
}

// Start starts the scheduler
func (s *Scheduler) Start() {
	if s.running.Load() {
		log.Warn("Expire scheduler already running")
		return
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running.Store(true)

	log.Info("Starting expire scheduler")

	// Start time wheel ticker
	s.wg.Add(1)
	go s.timeWheelTicker()

	// Start active expiration cycle
	s.wg.Add(1)
	go s.activeExpireCycle()
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	if !s.running.Load() {
		return
	}

	log.Info("Stopping expire scheduler")

	s.cancel()
	s.wg.Wait()

	s.running.Store(false)
	s.mgr.Stop()

	log.Info("Expire scheduler stopped")
}

// Running returns whether the scheduler is running
func (s *Scheduler) Running() bool {
	return s.running.Load()
}

// timeWheelTicker advances the time wheel at regular intervals
func (s *Scheduler) timeWheelTicker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			entries := s.mgr.Tick()
			if len(entries) > 0 {
				s.mgr.ProcessExpired(entries)
			}
		}
	}
}

// activeExpireCycle runs active expiration scanning
// This mimics Redis's activeExpireCycle
func (s *Scheduler) activeExpireCycle() {
	defer s.wg.Done()

	// Use a histogram-like approach with fast/slow cycles
	fastCycle := true
	fastTicker := time.NewTicker(s.config.FastCycleInterval)
	slowTicker := time.NewTicker(s.config.ActiveExpireInterval)
	defer fastTicker.Stop()
	defer slowTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-fastTicker.C:
			if fastCycle {
				s.runActiveExpireCycle(true)
			}
		case <-slowTicker.C:
			s.runActiveExpireCycle(false)
		}
	}
}

// runActiveExpireCycle runs a single active expiration cycle
func (s *Scheduler) runActiveExpireCycle(fast bool) {
	stats := s.mgr.Stats()

	// Skip if we don't have many expired keys
	if stats.TrackedKeys == 0 {
		return
	}

	// Calculate effort based on expired percentage
	expiredPercent := float64(stats.ExpiredCount) / float64(stats.TrackedKeys+1) * 100

	// If expired percentage is high, do more aggressive expiration
	effort := 20
	if expiredPercent > 10 {
		effort = 40
	}
	if expiredPercent > 25 {
		effort = 100
	}

	// The databases to scan would be passed from the DB selector
	// For now, we'll track this via the manager
	_ = effort

	// Toggle fast cycle
	if fast {
		// After a fast cycle, wait before the next one
		if expiredPercent < 10 {
			// If few expired keys, slow down
		}
	}
}

// Stats returns scheduler statistics
func (s *Scheduler) Stats() SchedulerStats {
	return SchedulerStats{
		Running: s.Running(),
		Manager: s.mgr.Stats(),
	}
}

// SchedulerStats holds scheduler statistics
type SchedulerStats struct {
	Running bool
	Manager ExpireStats
}
