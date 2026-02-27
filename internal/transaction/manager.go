// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction

import (
	"errors"
	"sync"

	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/net"
)

// QueuedCommand represents a command queued in a transaction
type QueuedCommand struct {
	CmdName string
	Args    []string
}

// Manager manages transaction state for connections
type Manager struct {
	mu sync.RWMutex

	// Transaction queues: connection -> queued commands
	queues map[*net.Conn][]*QueuedCommand

	// WATCHed keys: connection -> set of watched keys
	watchedKeys map[*net.Conn]map[string]struct{}

	// Dirty keys: keys that have been modified (for WATCH)
	dirtyKeys map[string]struct{}
}

// NewManager creates a new transaction manager
func NewManager() *Manager {
	return &Manager{
		queues:      make(map[*net.Conn][]*QueuedCommand),
		watchedKeys: make(map[*net.Conn]map[string]struct{}),
		dirtyKeys:   make(map[string]struct{}),
	}
}

// IsInTransaction returns true if the connection is in MULTI state
func (m *Manager) IsInTransaction(conn *net.Conn) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.queues[conn]
	return ok
}

// Begin starts a transaction for the connection
func (m *Manager) Begin(conn *net.Conn) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[conn]; ok {
		return ErrMultiNested
	}

	m.queues[conn] = make([]*QueuedCommand, 0, 10)
	return nil
}

// Queue adds a command to the transaction queue
func (m *Manager) Queue(conn *net.Conn, cmdName string, args []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if queue, ok := m.queues[conn]; ok {
		queue = append(queue, &QueuedCommand{
			CmdName: cmdName,
			Args:    args,
		})
		m.queues[conn] = queue
	}
}

// GetQueueLength returns the number of queued commands for a connection
func (m *Manager) GetQueueLength(conn *net.Conn) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if queue, ok := m.queues[conn]; ok {
		return len(queue)
	}
	return 0
}

// GetQueue returns the queued commands for a connection
func (m *Manager) GetQueue(conn *net.Conn) []*QueuedCommand {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if queue, ok := m.queues[conn]; ok {
		// Return a copy
		result := make([]*QueuedCommand, len(queue))
		copy(result, queue)
		return result
	}
	return nil
}

// Discard discards the transaction queue for a connection
func (m *Manager) Discard(conn *net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.queues, conn)
}

// Watch adds keys to the watch list for a connection
func (m *Manager) Watch(conn *net.Conn, keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watchedKeys[conn] == nil {
		m.watchedKeys[conn] = make(map[string]struct{})
	}

	// Also add to connection's watched keys
	watched := conn.GetWatchedKeys()
	for _, key := range keys {
		m.watchedKeys[conn][key] = struct{}{}
		watched[key] = struct{}{}
		// Clear dirty flag for this key - we start watching from now
		delete(m.dirtyKeys, key)
	}
}

// Unwatch removes keys from the watch list for a connection
func (m *Manager) Unwatch(conn *net.Conn, keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watchedKeys[conn] == nil {
		return
	}

	// Remove from connection's watched keys
	watched := conn.GetWatchedKeys()
	for _, key := range keys {
		delete(m.watchedKeys[conn], key)
		delete(watched, key)
	}

	// Clean up if empty
	if len(m.watchedKeys[conn]) == 0 {
		delete(m.watchedKeys, conn)
	}
}

// UnwatchAll removes all watched keys for a connection
func (m *Manager) UnwatchAll(conn *net.Conn) {
	m.mu.Lock()
	delete(m.watchedKeys, conn)
	m.mu.Unlock()

	// Call conn.UnwatchAll() outside the lock to avoid potential deadlock
	// if conn.UnwatchAll() tries to access transaction manager methods
	conn.UnwatchAll()
}

// MarkDirty marks a key as dirty (modified)
func (m *Manager) MarkDirty(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dirtyKeys[key] = struct{}{}
	// Debug: log dirty key
	// println("[DEBUG] MarkDirty:", key)
}

// CheckWatchedKeys checks if any watched keys have been modified
// Returns true if the transaction should be aborted
func (m *Manager) CheckWatchedKeys(conn *net.Conn) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	watched, ok := m.watchedKeys[conn]
	if !ok || len(watched) == 0 {
		return false
	}

	// Check if any watched key is dirty
	for key := range watched {
		if _, dirty := m.dirtyKeys[key]; dirty {
			return true
		}
	}

	return false
}

// ClearDirty clears the dirty keys list
func (m *Manager) ClearDirty() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dirtyKeys = make(map[string]struct{})
}

// ClearWatchedDirty clears dirty keys that are no longer watched by any connection
func (m *Manager) ClearWatchedDirty(conn *net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the keys that were watched by this connection
	watched, ok := m.watchedKeys[conn]
	if !ok {
		return
	}

	// For each watched key, check if any other connection is still watching it
	for key := range watched {
		stillWatched := false
		for _, otherWatched := range m.watchedKeys {
			if _, ok := otherWatched[key]; ok {
				stillWatched = true
				break
			}
		}
		// If no other connection is watching this key, clear the dirty flag
		if !stillWatched {
			delete(m.dirtyKeys, key)
		}
	}
}

// RemoveConnection cleans up transaction state for a closed connection
func (m *Manager) RemoveConnection(conn *net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.queues, conn)
	delete(m.watchedKeys, conn)
}

// Execute executes the queued commands for a connection
// The dispatcherFunc should execute a command and return a reply
func (m *Manager) Execute(conn *net.Conn, db *database.DB, dispatcherFunc interface{}) ([]interface{}, error) {
	m.mu.Lock()
	queue, ok := m.queues[conn]
	if !ok {
		m.mu.Unlock()
		return nil, ErrExecWithoutMulti
	}

	// Check if watched keys were modified
	if m.CheckWatchedKeys(conn) {
		m.mu.Unlock()
		return nil, ErrExecWatch
	}

	// Get a copy of the queue and clear the original
	commands := make([]*QueuedCommand, len(queue))
	copy(commands, queue)
	m.queues[conn] = nil

	// Clear watched keys for this connection (EXEC clears WATCH)
	watchedKeys := m.watchedKeys[conn]
	delete(m.watchedKeys, conn)

	m.mu.Unlock()

	// Execute commands outside the lock
	replies := make([]interface{}, len(commands))

	// The dispatcherFunc is expected to have the form:
	// func(cmdName string, args []string) (*command.Reply, error)
	// We need to handle this via interface{}
	for i, cmd := range commands {
		// This will be handled by the transaction command
		replies[i] = []interface{}{cmd.CmdName, cmd.Args}
	}

	// If all commands succeeded, clear the watched keys
	if watchedKeys != nil {
		for key := range watchedKeys {
			// Only clear if no other connection is watching this key
			m.mu.Lock()
			stillWatched := false
			for _, wk := range m.watchedKeys {
				if _, ok := wk[key]; ok {
					stillWatched = true
					break
				}
			}
			if !stillWatched {
				delete(m.dirtyKeys, key)
			}
			m.mu.Unlock()
		}
	}

	return replies, nil
}

// Transaction errors
var (
	ErrMultiNested = errors.New("ERR MULTI calls can not be nested")

	ErrExecWithoutMulti = errors.New("ERR EXEC without MULTI")

	ErrExecWatch = errors.New("WATCH inside MULTI is not allowed")

	ErrWatch = errors.New("EXECABORT Transaction discarded because of a previous error.")
)
