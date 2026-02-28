// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package expire

import (
	"sync"
	"time"
)

// TimeWheel is a hierarchical time wheel for efficient expiration management
// Based on the design of Kafka's timing wheel and Netty's HashedWheelTimer
type TimeWheel struct {
	sync.Mutex

	// Time wheel configuration
	tick      time.Duration // Tick duration
	wheelSize int64         // Number of slots in the wheel

	// Current state
	currentTick int64     // Current tick position
	startTime   time.Time // Start time of the wheel

	// The wheel buckets
	buckets []*Bucket

	// Overflow wheel for longer timeouts
	overflow *TimeWheel

	// For stopping the wheel
	stopped bool
}

// Bucket holds keys that expire at the same time slot
type Bucket struct {
	sync.Mutex
	keys map[string]struct{}
}

// NewTimeWheel creates a new time wheel
// tickMs: tick duration in milliseconds
// wheelSize: number of slots in the wheel
func NewTimeWheel(tickMs int64, wheelSize int64) *TimeWheel {
	if tickMs <= 0 {
		tickMs = 10 // Default 10ms tick
	}
	if wheelSize <= 0 {
		wheelSize = 512 // Default 512 slots
	}

	buckets := make([]*Bucket, wheelSize)
	for i := int64(0); i < wheelSize; i++ {
		buckets[i] = &Bucket{
			keys: make(map[string]struct{}),
		}
	}

	return &TimeWheel{
		tick:        time.Duration(tickMs) * time.Millisecond,
		wheelSize:   wheelSize,
		currentTick: 0,
		startTime:   time.Now(),
		buckets:     buckets,
		stopped:     false,
	}
}

// Add adds a key to expire at a specific deadline
func (tw *TimeWheel) Add(key string, deadline time.Time) {
	tw.Lock()
	defer tw.Unlock()

	if tw.stopped {
		return
	}

	// Calculate the number of ticks until expiration
	expiration := deadline.Sub(time.Now())
	if expiration <= 0 {
		// Already expired, add to current bucket for immediate processing
		tw.buckets[tw.currentTick%tw.wheelSize].Add(key)
		return
	}

	// Calculate the tick number for this expiration
	ticks := expiration.Nanoseconds() / tw.tick.Nanoseconds()

	// Calculate the slot index
	idx := (tw.currentTick + ticks) % tw.wheelSize

	// If the expiration is beyond the current wheel's capacity,
	// add to overflow wheel
	if ticks >= tw.wheelSize {
		if tw.overflow == nil {
			// Create overflow wheel with larger tick duration
			tw.overflow = NewTimeWheel(int64(tw.tick.Milliseconds())*int64(tw.wheelSize), tw.wheelSize)
		}
		tw.overflow.Add(key, deadline)
		return
	}

	tw.buckets[idx].Add(key)
}

// Remove removes a key from the time wheel
func (tw *TimeWheel) Remove(key string) {
	tw.Lock()
	defer tw.Unlock()

	if tw.stopped {
		return
	}

	// Try to remove from all buckets
	for _, bucket := range tw.buckets {
		bucket.Remove(key)
	}

	// Also check overflow wheel
	if tw.overflow != nil {
		tw.overflow.Remove(key)
	}
}

// Advance advances the time wheel and returns expired keys
func (tw *TimeWheel) Advance() []string {
	tw.Lock()
	defer tw.Unlock()

	if tw.stopped {
		return nil
	}

	// Move to next tick
	tw.currentTick++

	// Get the current bucket
	idx := tw.currentTick % tw.wheelSize
	bucket := tw.buckets[idx]

	// Get and clear expired keys
	expired := bucket.GetAndClear()

	// If we completed a full rotation, advance overflow wheel
	if idx == 0 && tw.overflow != nil {
		overflowExpired := tw.overflow.Advance()
		expired = append(expired, overflowExpired...)
	}

	return expired
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.Lock()
	defer tw.Unlock()

	tw.stopped = true

	if tw.overflow != nil {
		tw.overflow.Stop()
	}
}

// Add adds a key to the bucket
func (b *Bucket) Add(key string) {
	b.Lock()
	defer b.Unlock()
	b.keys[key] = struct{}{}
}

// Remove removes a key from the bucket
func (b *Bucket) Remove(key string) {
	b.Lock()
	defer b.Unlock()
	delete(b.keys, key)
}

// GetAndClear returns all keys in the bucket and clears it
func (b *Bucket) GetAndClear() []string {
	b.Lock()
	defer b.Unlock()

	if len(b.keys) == 0 {
		return nil
	}

	keys := make([]string, 0, len(b.keys))
	for key := range b.keys {
		keys = append(keys, key)
	}

	// Clear the bucket
	b.keys = make(map[string]struct{})

	return keys
}

// Size returns the approximate number of keys in the time wheel
func (tw *TimeWheel) Size() int {
	tw.Lock()
	defer tw.Unlock()

	count := 0
	for _, bucket := range tw.buckets {
		bucket.Lock()
		count += len(bucket.keys)
		bucket.Unlock()
	}

	if tw.overflow != nil {
		count += tw.overflow.Size()
	}

	return count
}
