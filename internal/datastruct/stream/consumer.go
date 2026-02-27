// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stream

import (
	"sync"
)

// ConsumerGroup represents a consumer group in a stream
type ConsumerGroup struct {
	name     string
	lastID   StreamID      // Last delivered ID
	consumers map[string]*Consumer
	mu       sync.RWMutex
}

// Consumer represents a consumer within a consumer group
type Consumer struct {
	name      string
	pendingIDs map[StreamID]int64 // Pending IDs with their timestamp
	mu        sync.RWMutex
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(name string, initialID StreamID) *ConsumerGroup {
	return &ConsumerGroup{
		name:      name,
		lastID:    initialID,
		consumers: make(map[string]*Consumer),
	}
}

// GetName returns the consumer group name
func (cg *ConsumerGroup) GetName() string {
	return cg.name
}

// GetLastID returns the last delivered ID
func (cg *ConsumerGroup) GetLastID() StreamID {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.lastID
}

// SetLastID sets the last delivered ID
func (cg *ConsumerGroup) SetLastID(id StreamID) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.lastID = id
}

// GetOrCreateConsumer gets or creates a consumer
func (cg *ConsumerGroup) GetOrCreateConsumer(name string) *Consumer {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if consumer, ok := cg.consumers[name]; ok {
		return consumer
	}

	consumer := &Consumer{
		name:       name,
		pendingIDs: make(map[StreamID]int64),
	}
	cg.consumers[name] = consumer
	return consumer
}

// RemoveConsumer removes a consumer
func (cg *ConsumerGroup) RemoveConsumer(name string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.consumers, name)
}

// GetConsumers returns all consumers
func (cg *ConsumerGroup) GetConsumers() map[string]*Consumer {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	result := make(map[string]*Consumer, len(cg.consumers))
	for k, v := range cg.consumers {
		result[k] = v
	}
	return result
}

// AddPendingID adds a pending ID for a consumer
func (cg *ConsumerGroup) AddPendingID(consumerName string, id StreamID, timestamp int64) {
	consumer := cg.GetOrCreateConsumer(consumerName)
	consumer.AddPendingID(id, timestamp)
}

// RemovePendingID removes a pending ID for a consumer
func (cg *ConsumerGroup) RemovePendingID(consumerName string, id StreamID) {
	cg.mu.RLock()
	consumer, ok := cg.consumers[consumerName]
	cg.mu.RUnlock()

	if ok {
		consumer.RemovePendingID(id)
	}
}

// GetPendingIDs returns pending IDs for a consumer
func (cg *ConsumerGroup) GetPendingIDs(consumerName string) map[StreamID]int64 {
	cg.mu.RLock()
	consumer, ok := cg.consumers[consumerName]
	cg.mu.RUnlock()

	if !ok {
		return nil
	}
	return consumer.GetPendingIDs()
}

// Size returns the approximate memory size
func (cg *ConsumerGroup) Size() int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	var size int64 = 64 // Base size
	for _, consumer := range cg.consumers {
		size += consumer.Size()
	}
	return size
}

// AddPendingID adds a pending ID
func (c *Consumer) AddPendingID(id StreamID, timestamp int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingIDs[id] = timestamp
}

// RemovePendingID removes a pending ID
func (c *Consumer) RemovePendingID(id StreamID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pendingIDs, id)
}

// GetName returns the consumer name
func (c *Consumer) GetName() string {
	return c.name
}

// GetPendingIDs returns all pending IDs
func (c *Consumer) GetPendingIDs() map[StreamID]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[StreamID]int64, len(c.pendingIDs))
	for k, v := range c.pendingIDs {
		result[k] = v
	}
	return result
}

// Size returns the approximate memory size
func (c *Consumer) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(len(c.pendingIDs)) * 32
}

// ConsumerGroupManager manages all consumer groups for a stream
type ConsumerGroupManager struct {
	groups map[string]*ConsumerGroup
	mu     sync.RWMutex
}

// NewConsumerGroupManager creates a new consumer group manager
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		groups: make(map[string]*ConsumerGroup),
	}
}

// CreateGroup creates a new consumer group
func (m *ConsumerGroupManager) CreateGroup(name string, initialID StreamID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.groups[name]; exists {
		return nil // Already exists, idempotent
	}

	m.groups[name] = NewConsumerGroup(name, initialID)
	return nil
}

// GetGroup gets a consumer group
func (m *ConsumerGroupManager) GetGroup(name string) (*ConsumerGroup, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, ok := m.groups[name]
	return group, ok
}

// DeleteGroup deletes a consumer group
func (m *ConsumerGroupManager) DeleteGroup(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.groups[name]; !exists {
		return false
	}

	delete(m.groups, name)
	return true
}

// GetGroups returns all consumer groups
func (m *ConsumerGroupManager) GetGroups() map[string]*ConsumerGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*ConsumerGroup, len(m.groups))
	for k, v := range m.groups {
		result[k] = v
	}
	return result
}

// Size returns the approximate memory size
func (m *ConsumerGroupManager) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var size int64 = 32 // Base size
	for _, group := range m.groups {
		size += group.Size()
	}
	return size
}
