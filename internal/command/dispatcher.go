// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package command

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/net"
	"github.com/zyhnesmr/godis/internal/protocol/resp"
	"github.com/zyhnesmr/godis/internal/transaction"
)

// AOFLogger is the interface for logging commands to AOF
type AOFLogger interface {
	LogCommand(db int, cmdName string, args []string) error
}

// Dispatcher dispatches commands to their handlers
type Dispatcher struct {
	commands   map[string]*Command
	mu         sync.RWMutex
	db         *database.DBSelector
	txManager  *transaction.Manager
	aofLogger  AOFLogger
}

// NewDispatcher creates a new command dispatcher
func NewDispatcher(db *database.DBSelector) *Dispatcher {
	return &Dispatcher{
		commands:  make(map[string]*Command),
		db:        db,
		txManager: transaction.NewManager(),
	}
}

// SetAOFLogger sets the AOF logger
func (d *Dispatcher) SetAOFLogger(logger AOFLogger) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.aofLogger = logger
}

// GetTxManager returns the transaction manager
func (d *Dispatcher) GetTxManager() *transaction.Manager {
	return d.txManager
}

// Register registers a new command
func (d *Dispatcher) Register(cmd *Command) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.commands[strings.ToLower(cmd.Name)] = cmd
}

// Get returns a command by name
func (d *Dispatcher) Get(name string) (*Command, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	cmd, ok := d.commands[strings.ToLower(name)]
	return cmd, ok
}

// Dispatch dispatches a command to its handler
func (d *Dispatcher) Dispatch(ctx context.Context, conn *net.Conn, cmdName string, args []string) ([]byte, error) {
	// Find command
	cmd, ok := d.Get(cmdName)
	if !ok {
		return resp.BuildErrorString(fmt.Sprintf("ERR unknown command '%s'", cmdName)), nil
	}

	// Check arity
	if err := cmd.CheckArity(len(args)); err != nil {
		return resp.BuildErrorString(err.Error()), nil
	}

	// Handle transaction commands
	switch strings.ToUpper(cmdName) {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		// These are always executed immediately
		return d.dispatchCommand(ctx, conn, cmd, args)
	}

	// Check if client is in MULTI state
	if d.txManager.IsInTransaction(conn) {
		// Queue the command
		d.txManager.Queue(conn, cmdName, args)
		return resp.BuildQueued(), nil
	}

	// Execute command normally
	return d.dispatchCommand(ctx, conn, cmd, args)
}

// dispatchCommand executes a command immediately
func (d *Dispatcher) dispatchCommand(ctx context.Context, conn *net.Conn, cmd *Command, args []string) ([]byte, error) {
	// Get database for this connection
	db, err := d.db.GetDB(conn.GetDB())
	if err != nil {
		return resp.BuildErrorString("ERR invalid DB index"), nil
	}

	// Create command context
	cmdCtx := &Context{
		DB:      db,
		Conn:    conn,
		CmdName: cmd.Name,
		Args:    args,
	}

	// Execute command
	reply, err := cmd.Handler(cmdCtx)
	if err != nil {
		return resp.BuildErrorString(err.Error()), nil
	}

	// Log to AOF if command succeeded and is a write command
	if !reply.IsError() && d.aofLogger != nil && cmd.HasFlag(FlagWrite) {
		if !isReadOnlyCommand(cmd.Name) {
			_ = d.aofLogger.LogCommand(conn.GetDB(), cmd.Name, args)
		}
	}

	return reply.Marshal(), nil
}

// DispatchCommand dispatches a single command (used by EXEC)
func (d *Dispatcher) DispatchCommand(ctx interface{}, conn *net.Conn, cmdName string, args []string) (*Reply, error) {
	cmd, ok := d.Get(cmdName)
	if !ok {
		return NewErrorReplyStr(fmt.Sprintf("ERR unknown command '%s'", cmdName)), nil
	}

	// Check arity
	if err := cmd.CheckArity(len(args)); err != nil {
		return NewErrorReply(err), nil
	}

	return d.dispatchCommandReply(context.Background(), conn, cmd, args)
}

// dispatchCommandReply executes a command and returns a Reply
func (d *Dispatcher) dispatchCommandReply(ctx context.Context, conn *net.Conn, cmd *Command, args []string) (*Reply, error) {
	// Get database for this connection
	db, err := d.db.GetDB(conn.GetDB())
	if err != nil {
		return NewErrorReplyStr("ERR invalid DB index"), nil
	}

	// Create command context
	cmdCtx := &Context{
		DB:      db,
		Conn:    conn,
		CmdName: cmd.Name,
		Args:    args,
	}

	// Execute command
	reply, err := cmd.Handler(cmdCtx)

	// Log to AOF if command succeeded and is a write command
	if err == nil && !reply.IsError() && d.aofLogger != nil && cmd.HasFlag(FlagWrite) {
		// Skip commands that don't modify data
		if !isReadOnlyCommand(cmd.Name) {
			_ = d.aofLogger.LogCommand(conn.GetDB(), cmd.Name, args)
		}
	}

	return reply, err
}

// isReadOnlyCommand returns true if the command is read-only (even if marked as write)
func isReadOnlyCommand(cmdName string) bool {
	readOnly := []string{
		"DBSIZE", "KEYS", "EXISTS", "TYPE", "TTL", "PTTL", "STRLEN",
		"GET", "MGET", "HEXISTS", "HGET", "HMGET", "HKEYS", "HVALS", "HLEN", "HSTRLEN", "HRANDFIELD",
		"LINDEX", "LLEN", "LRANGE",
		"SISMEMBER", "SMISMEMBER", "SCARD", "SRANDMEMBER", "SMEMBERS", "SSCAN",
		"ZSCORE", "ZMSCORE", "ZCARD", "ZCOUNT", "ZRANK", "ZREVRANK", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZSCAN", "ZRANDMEMBER",
		"PUBSUB", "PING", "ECHO",
	}

	for _, roc := range readOnly {
		if strings.ToUpper(cmdName) == roc {
			return true
		}
	}
	return false
}

// ProcessCommand processes a command (compatibility interface)
func (d *Dispatcher) ProcessCommand(ctx context.Context, conn *net.Conn, cmdName string, args []string) ([]byte, error) {
	return d.Dispatch(ctx, conn, cmdName, args)
}

// Commands returns all registered commands
func (d *Dispatcher) Commands() map[string]*Command {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string]*Command, len(d.commands))
	for k, v := range d.commands {
		result[k] = v
	}
	return result
}

// GetDB returns the database selector
func (d *Dispatcher) GetDB() *database.DBSelector {
	return d.db
}
