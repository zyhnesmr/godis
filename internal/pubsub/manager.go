// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pubsub

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/zyhnesmr/godis/internal/net"
)

// Manager manages publish/subscribe subscriptions
type Manager struct {
	mu           sync.RWMutex
	channels     map[string]*channelSubscribers    // channel -> subscribers
	patternConns map[string]map[*net.Conn]struct{} // pattern -> connections
	connPatterns map[*net.Conn]map[string]struct{} // connection -> patterns
}

// channelSubscribers manages subscribers for a single channel
type channelSubscribers struct {
	mu          sync.RWMutex
	subscribers map[*net.Conn]struct{}
}

// newChannelSubscribers creates a new channel subscribers
func newChannelSubscribers() *channelSubscribers {
	return &channelSubscribers{
		subscribers: make(map[*net.Conn]struct{}),
	}
}

// NewManager creates a new pubsub manager
func NewManager() *Manager {
	return &Manager{
		channels:     make(map[string]*channelSubscribers),
		patternConns: make(map[string]map[*net.Conn]struct{}),
		connPatterns: make(map[*net.Conn]map[string]struct{}),
	}
}

// Subscribe adds a connection to a channel's subscribers
func (m *Manager) Subscribe(conn *net.Conn, channels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ch := range channels {
		if m.channels[ch] == nil {
			m.channels[ch] = newChannelSubscribers()
		}
		m.channels[ch].add(conn)
		conn.Subscribe(ch)
	}
}

// Unsubscribe removes a connection from channel subscribers
func (m *Manager) Unsubscribe(conn *net.Conn, channels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(channels) == 0 {
		// Unsubscribe from all channels
		for channel := range conn.GetSubscriptions() {
			m.unsubscribe(conn, channel)
		}
		return
	}

	for _, channel := range channels {
		m.unsubscribe(conn, channel)
	}
}

// unsubscribe removes a connection from a specific channel
func (m *Manager) unsubscribe(conn *net.Conn, channel string) {
	if subs, ok := m.channels[channel]; ok {
		subs.remove(conn)
		if subs.isEmpty() {
			delete(m.channels, channel)
		}
	}
	conn.Unsubscribe(channel)
}

// PSubscribe adds a connection to pattern subscribers
func (m *Manager) PSubscribe(conn *net.Conn, patterns ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pattern := range patterns {
		if m.patternConns[pattern] == nil {
			m.patternConns[pattern] = make(map[*net.Conn]struct{})
		}
		m.patternConns[pattern][conn] = struct{}{}

		if m.connPatterns[conn] == nil {
			m.connPatterns[conn] = make(map[string]struct{})
		}
		m.connPatterns[conn][pattern] = struct{}{}

		conn.PSubscribe(pattern)
	}
}

// PUnsubscribe removes a connection from pattern subscribers
func (m *Manager) PUnsubscribe(conn *net.Conn, patterns ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(patterns) == 0 {
		// Unsubscribe from all patterns
		for pattern := range conn.GetPatterns() {
			m.punsubscribe(conn, pattern)
		}
		return
	}

	for _, pattern := range patterns {
		m.punsubscribe(conn, pattern)
	}
}

// punsubscribe removes a connection from a specific pattern
func (m *Manager) punsubscribe(conn *net.Conn, pattern string) {
	if patternMap, ok := m.patternConns[pattern]; ok {
		delete(patternMap, conn)
		if len(patternMap) == 0 {
			delete(m.patternConns, pattern)
		}
	}
	if patMap, ok := m.connPatterns[conn]; ok {
		delete(patMap, pattern)
		if len(patMap) == 0 {
			delete(m.connPatterns, conn)
		}
	}
	conn.PUnsubscribe(pattern)
}

// Publish sends a message to all subscribers of a channel
// Returns the number of subscribers that received the message
func (m *Manager) Publish(channel string, message []byte) int {
	m.mu.RLock()
	subs, ok := m.channels[channel]
	m.mu.RUnlock()

	if !ok {
		return 0
	}

	// Get list of subscribers to notify
	subs.mu.RLock()
	conns := make([]*net.Conn, 0, len(subs.subscribers))
	for conn := range subs.subscribers {
		conns = append(conns, conn)
	}
	subs.mu.RUnlock()

	// Send message to each subscriber
	count := 0
	for _, conn := range conns {
		if !conn.IsClosed() {
			if m.publishToConn(conn, channel, message) {
				count++
			}
		}
	}

	// Also publish to matching pattern subscriptions
	m.publishToPatterns(channel, message)

	return count
}

// publishToConn sends a message to a single connection
func (m *Manager) publishToConn(conn *net.Conn, channel string, message []byte) bool {
	// Build the message array: ["message", "channel", "payload"]
	// Use strings.Builder for efficiency
	var builder strings.Builder
	builder.WriteString("*3\r\n")
	builder.WriteString("$7\r\nmessage\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(channel)))
	builder.WriteString("\r\n")
	builder.WriteString(channel)
	builder.WriteString("\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(message)))
	builder.WriteString("\r\n")
	builder.Write(message)
	builder.WriteString("\r\n")

	data := builder.String()

	err := conn.WriteRESP([]byte(data))
	if err != nil {
		return false
	}
	return conn.Flush() == nil
}

// publishToPatterns sends a message to matching pattern subscriptions
func (m *Manager) publishToPatterns(channel string, message []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find matching patterns and send to subscribed connections
	for pattern, conns := range m.patternConns {
		if matchPattern(pattern, channel) {
			for conn := range conns {
				if !conn.IsClosed() {
					_ = m.PublishToPattern(conn, pattern, channel, message)
				}
			}
		}
	}
}

// PublishToPattern sends a message to a specific pattern subscriber
func (m *Manager) PublishToPattern(conn *net.Conn, pattern, channel string, message []byte) error {
	// Build the message array: ["pmessage", "pattern", "channel", "payload"]
	var builder strings.Builder
	builder.WriteString("*4\r\n")
	builder.WriteString("$9\r\npmessage\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(pattern)))
	builder.WriteString("\r\n")
	builder.WriteString(pattern)
	builder.WriteString("\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(channel)))
	builder.WriteString("\r\n")
	builder.WriteString(channel)
	builder.WriteString("\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(message)))
	builder.WriteString("\r\n")
	builder.Write(message)
	builder.WriteString("\r\n")

	err := conn.WriteRESP([]byte(builder.String()))
	if err != nil {
		return err
	}
	return conn.Flush()
}

// NumSubscribers returns the number of subscribers for the given channels
func (m *Manager) NumSubscribers(channels ...string) map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]int)
	for _, channel := range channels {
		if subs, ok := m.channels[channel]; ok {
			subs.mu.RLock()
			result[channel] = len(subs.subscribers)
			subs.mu.RUnlock()
		} else {
			result[channel] = 0
		}
	}
	return result
}

// NumChannels returns the number of active channels
func (m *Manager) NumChannels() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels)
}

// NumPatterns returns the number of active pattern subscriptions
func (m *Manager) NumPatterns() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.patternConns)
}

// ListChannels returns a list of all active channels
func (m *Manager) ListChannels() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	channels := make([]string, 0, len(m.channels))
	for channel := range m.channels {
		channels = append(channels, channel)
	}
	return channels
}

// RemoveConn removes a connection from all subscriptions (called when connection closes)
func (m *Manager) RemoveConn(conn *net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from all channels
	for channel, subs := range m.channels {
		subs.remove(conn)
		if subs.isEmpty() {
			delete(m.channels, channel)
		}
	}

	// Remove from all patterns
	for pattern, conns := range m.patternConns {
		delete(conns, conn)
		if len(conns) == 0 {
			delete(m.patternConns, pattern)
		}
	}
	delete(m.connPatterns, conn)
}

// add adds a subscriber to the channel
func (c *channelSubscribers) add(conn *net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscribers[conn] = struct{}{}
}

// remove removes a subscriber from the channel
func (c *channelSubscribers) remove(conn *net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscribers, conn)
}

// isEmpty returns true if there are no subscribers
func (c *channelSubscribers) isEmpty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscribers) == 0
}

// matchPattern checks if a channel matches a glob pattern
func matchPattern(pattern, channel string) bool {
	// Handle glob-style patterns: *, ?, [], etc.
	// For simplicity, we'll use filepath.Match which supports similar patterns
	matched, _ := filepath.Match(pattern, channel)
	return matched
}

// GetConnSubscriptions returns the channels a connection is subscribed to
func (m *Manager) GetConnSubscriptions(conn *net.Conn) []string {
	subs := conn.GetSubscriptions()
	channels := make([]string, 0, len(subs))
	for channel := range subs {
		channels = append(channels, channel)
	}
	return channels
}

// GetConnPatterns returns the patterns a connection is subscribed to
func (m *Manager) GetConnPatterns(conn *net.Conn) []string {
	patterns := conn.GetPatterns()
	patternList := make([]string, 0, len(patterns))
	for pattern := range patterns {
		patternList = append(patternList, pattern)
	}
	return patternList
}
