// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import (
	"bufio"
	"io"
	"net"
	"sync"
	"time"

	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// Conn wraps a network connection with buffering
type Conn struct {
	rawConn net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer

	// Connection metadata
	id         uint64
	createdAt  time.Time
	lastActive time.Time

	// Connection state
	mu     sync.Mutex
	closed bool

	// Client info
	name  string
	flags uint32

	// Database selection
	db int

	// Transaction state
	inMulti     bool
	inWatch     bool
	watchedKeys map[string]struct{}

	// Subscription state
	subscriptions map[string]struct{}
	patterns      map[string]struct{}

	// Query buffer
	queryBuffer []byte

	// Response queue
	respQueue [][]byte
}

const (
	// FlagClient is set for normal clients
	FlagClient uint32 = 1 << iota

	// FlagSlave is set for replica clients
	FlagSlave

	// FlagMaster is set for master connection
	FlagMaster

	// FlagPubSub is set for clients in pub/sub mode
	FlagPubSub

	// FlagMulti is set for clients in MULTI/EXEC transaction
	FlagMulti

	// FlagDirty is set when EXEC should fail due to watched keys
	FlagDirty

	// Default buffer sizes
	defaultReadBufferSize  = 16 * 1024   // 16KB
	defaultWriteBufferSize = 16 * 1024   // 16KB
	maxQueryBufferSize     = 1024 * 1024 // 1MB
)

// NewConn creates a new connection wrapper
func NewConn(rawConn net.Conn) *Conn {
	return &Conn{
		rawConn:       rawConn,
		reader:        bufio.NewReaderSize(rawConn, defaultReadBufferSize),
		writer:        bufio.NewWriterSize(rawConn, defaultWriteBufferSize),
		createdAt:     time.Now(),
		lastActive:    time.Now(),
		db:            0,
		watchedKeys:   make(map[string]struct{}),
		subscriptions: make(map[string]struct{}),
		patterns:      make(map[string]struct{}),
		queryBuffer:   make([]byte, 0, 512),
		flags:         FlagClient,
	}
}

// Read reads data from the connection
func (c *Conn) Read(b []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.mu.Unlock()

	n, err := c.reader.Read(b)
	if err != nil {
		return n, err
	}

	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()

	return n, nil
}

// Write writes data to the connection
func (c *Conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, io.ErrClosedPipe
	}

	return c.writer.Write(b)
}

// ReadLine reads a line ending with \r\n
func (c *Conn) ReadLine() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return "", io.EOF
	}

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", io.ErrUnexpectedEOF
	}

	c.lastActive = time.Now()

	return line[:len(line)-2], nil
}

// ReadBytes reads exactly n bytes from the connection
func (c *Conn) ReadBytes(n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, io.EOF
	}

	buf := make([]byte, n)
	_, err := io.ReadFull(c.reader, buf)
	if err != nil {
		return nil, err
	}

	// Read trailing \r\n
	crlf := make([]byte, 2)
	_, err = io.ReadFull(c.reader, crlf)
	if err != nil {
		return nil, err
	}

	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, io.ErrUnexpectedEOF
	}

	c.lastActive = time.Now()

	return buf, nil
}

// Flush flushes the write buffer
func (c *Conn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	return c.writer.Flush()
}

// Close closes the connection
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Flush any pending data
	_ = c.writer.Flush()

	return c.rawConn.Close()
}

// IsClosed returns true if the connection is closed
func (c *Conn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// RemoteAddr returns the remote address
func (c *Conn) RemoteAddr() net.Addr {
	return c.rawConn.RemoteAddr()
}

// LocalAddr returns the local address
func (c *Conn) LocalAddr() net.Addr {
	return c.rawConn.LocalAddr()
}

// SetDeadline sets the read and write deadlines
func (c *Conn) SetDeadline(t time.Time) error {
	return c.rawConn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.rawConn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.rawConn.SetWriteDeadline(t)
}

// GetID returns the connection ID
func (c *Conn) GetID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// SetID sets the connection ID
func (c *Conn) SetID(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id = id
}

// GetCreatedAt returns the creation time
func (c *Conn) GetCreatedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createdAt
}

// GetLastActive returns the last active time
func (c *Conn) GetLastActive() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastActive
}

// GetName returns the client name
func (c *Conn) GetName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

// SetName sets the client name
func (c *Conn) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name = name
}

// GetDB returns the selected database
func (c *Conn) GetDB() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db
}

// SetDB sets the selected database
func (c *Conn) SetDB(db int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.db = db
}

// GetFlags returns the connection flags
func (c *Conn) GetFlags() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags
}

// SetFlags sets the connection flags
func (c *Conn) SetFlags(flags uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flags = flags
}

// HasFlag checks if a flag is set
func (c *Conn) HasFlag(flag uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags&flag != 0
}

// AddFlag adds a flag
func (c *Conn) AddFlag(flag uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flags |= flag
}

// RemoveFlag removes a flag
func (c *Conn) RemoveFlag(flag uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flags &= ^flag
}

// IsInMulti returns true if client is in MULTI/EXEC
func (c *Conn) IsInMulti() bool {
	return c.HasFlag(FlagMulti)
}

// SetInMulti sets the MULTI state
func (c *Conn) SetInMulti(inMulti bool) {
	if inMulti {
		c.AddFlag(FlagMulti)
	} else {
		c.RemoveFlag(FlagMulti)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inMulti = inMulti
}

// IsInPubSub returns true if client is in pub/sub mode
func (c *Conn) IsInPubSub() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subscriptions) > 0 || len(c.patterns) > 0
}

// GetSubscriptions returns the subscriptions map
func (c *Conn) GetSubscriptions() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscriptions
}

// GetPatterns returns the pattern subscriptions map
func (c *Conn) GetPatterns() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.patterns
}

// Subscribe subscribes to a channel
func (c *Conn) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscriptions[channel] = struct{}{}
}

// Unsubscribe unsubscribes from a channel
func (c *Conn) Unsubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscriptions, channel)
}

// PSubscribe subscribes to a pattern
func (c *Conn) PSubscribe(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.patterns[pattern] = struct{}{}
}

// PUnsubscribe unsubscribes from a pattern
func (c *Conn) PUnsubscribe(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.patterns, pattern)
}

// GetWatchedKeys returns the watched keys
func (c *Conn) GetWatchedKeys() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watchedKeys
}

// WatchKey adds a key to the watch list
func (c *Conn) WatchKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watchedKeys == nil {
		c.watchedKeys = make(map[string]struct{})
	}
	c.watchedKeys[key] = struct{}{}
}

// UnwatchAll clears the watched keys
func (c *Conn) UnwatchAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watchedKeys = make(map[string]struct{})
	c.RemoveFlag(FlagDirty)
}

// MarkDirty marks the transaction as dirty (watched key was modified)
func (c *Conn) MarkDirty() {
	c.AddFlag(FlagDirty)
}

// IsDirty returns true if watched keys were modified
func (c *Conn) IsDirty() bool {
	return c.HasFlag(FlagDirty)
}

// NewRESPParser creates a new RESP parser for this connection
func (c *Conn) NewRESPParser() *resp.Parser {
	return resp.NewParser(c.reader)
}

// WriteRESP writes a RESP message to the connection
func (c *Conn) WriteRESP(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	_, err := c.writer.Write(data)
	return err
}

// WriteRESPMessage writes a RESP message
func (c *Conn) WriteRESPMessage(msg *resp.Message) error {
	return c.WriteRESP(msg.Marshal())
}

// GetRawConn returns the underlying connection
func (c *Conn) GetRawConn() net.Conn {
	return c.rawConn
}

// SetReadBufferSize sets the read buffer size
func (c *Conn) SetReadBufferSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if size > 0 {
		c.reader.Reset(c.rawConn)
		c.reader = bufio.NewReaderSize(c.rawConn, size)
	}
}

// SetWriteBufferSize sets the write buffer size
func (c *Conn) SetWriteBufferSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if size > 0 {
		_ = c.writer.Flush()
		c.writer.Reset(c.rawConn)
		c.writer = bufio.NewWriterSize(c.rawConn, size)
	}
}
