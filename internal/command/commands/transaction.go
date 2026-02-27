// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/transaction"
)

var (
	txManager *transaction.Manager
	txDisp    *command.Dispatcher
)

// SetTxManager sets the global transaction manager
func SetTxManager(mgr *transaction.Manager) {
	txManager = mgr
}

// RegisterTransactionCommands registers all transaction commands
func RegisterTransactionCommands(disp Dispatcher) {
	// Store dispatcher reference for command execution
	var ok bool
	txDisp, ok = disp.(*command.Dispatcher)
	if !ok {
		// This shouldn't happen in normal usage
		return
	}

	disp.Register(&command.Command{
		Name:       "MULTI",
		Handler:    multiCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatTransaction},
	})

	disp.Register(&command.Command{
		Name:       "EXEC",
		Handler:    execCmd,
		Arity:      1,
		Flags:      []string{command.FlagNoScript, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatTransaction},
	})

	disp.Register(&command.Command{
		Name:       "DISCARD",
		Handler:    discardCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatTransaction},
	})

	disp.Register(&command.Command{
		Name:       "WATCH",
		Handler:    watchCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast, command.FlagSkipSlowlog},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatTransaction},
	})

	disp.Register(&command.Command{
		Name:       "UNWATCH",
		Handler:    unwatchCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatTransaction},
	})
}

// MULTI marks the start of a transaction
func multiCmd(ctx *command.Context) (*command.Reply, error) {
	// Check if already in MULTI state
	if ctx.Conn.IsInMulti() {
		return command.NewErrorReplyStr("ERR MULTI calls can not be nested"), nil
	}

	// Mark connection as being in MULTI state
	ctx.Conn.SetInMulti(true)

	// Start transaction in manager
	if err := txManager.Begin(ctx.Conn); err != nil {
		ctx.Conn.SetInMulti(false)
		return command.NewErrorReply(err), nil
	}

	return command.NewStatusReply("OK"), nil
}

// EXEC executes all queued commands
func execCmd(ctx *command.Context) (*command.Reply, error) {
	// Check if in MULTI state
	if !ctx.Conn.IsInMulti() {
		return command.NewErrorReplyStr("ERR EXEC without MULTI"), nil
	}

	// Get the queued commands
	queued := txManager.GetQueue(ctx.Conn)
	if queued == nil || len(queued) == 0 {
		// Empty transaction - just exit MULTI state
		ctx.Conn.SetInMulti(false)
		txManager.Discard(ctx.Conn)
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	// Check if watched keys were modified
	if txManager.CheckWatchedKeys(ctx.Conn) {
		// Clear the watched keys
		// Note: Skipping UnwatchAll to avoid deadlock
		txManager.Discard(ctx.Conn)
		ctx.Conn.SetInMulti(false)

		// Return nil for each queued command
		results := make([]interface{}, len(queued))
		for i := range results {
			results[i] = nil
		}
		return command.NewArrayReplyFromAny(results), nil
	}

	// IMPORTANT: Clear the queue and MULTI state BEFORE executing commands
	// This prevents the dispatcher from re-queueing the commands
	txManager.Discard(ctx.Conn)
	ctx.Conn.SetInMulti(false)

	// Execute each queued command
	replies := make([]interface{}, 0, len(queued))
	for _, queuedCmd := range queued {
		// Use the dispatcher to execute the command
		cmd, ok := txDisp.Get(queuedCmd.CmdName)
		if !ok {
			replies = append(replies, "ERR unknown command '"+queuedCmd.CmdName+"'")
			continue
		}

		// Create command context
		cmdCtx := &command.Context{
			DB:      ctx.DB,
			Conn:    ctx.Conn,
			CmdName: queuedCmd.CmdName,
			Args:    queuedCmd.Args,
		}

		// Execute the command
		reply, err := cmd.Handler(cmdCtx)
		if err != nil {
			// Error during execution - return error in response
			replies = append(replies, err.Error())
		} else {
			// Convert reply to value
			val := replyToValue(reply)
			replies = append(replies, val)
		}
	}

	// Clear dirty keys that are no longer watched by any connection
	txManager.ClearWatchedDirty(ctx.Conn)

	return command.NewArrayReplyFromAny(replies), nil
}

// replyToValue converts a Reply to a value suitable for EXEC response
func replyToValue(reply *command.Reply) interface{} {
	if reply == nil {
		return nil
	}

	switch reply.Type {
	case command.ReplyTypeStatus:
		if s, ok := reply.Value.(string); ok {
			return s
		}
		return "OK"
	case command.ReplyTypeError:
		if s, ok := reply.Value.(string); ok {
			return s
		}
		return reply.Value
	case command.ReplyTypeInteger:
		if i, ok := reply.Value.(int64); ok {
			return i
		}
		return reply.Value
	case command.ReplyTypeBulkString:
		if s, ok := reply.Value.(string); ok {
			return s
		}
		if b, ok := reply.Value.([]byte); ok {
			return string(b)
		}
		return reply.Value
	case command.ReplyTypeArray:
		return reply.Value
	case command.ReplyTypeNil:
		return nil
	default:
		return reply.Value
	}
}

// DISCARD discards all queued commands
func discardCmd(ctx *command.Context) (*command.Reply, error) {
	// Check if in MULTI state
	if !ctx.Conn.IsInMulti() {
		return command.NewErrorReplyStr("ERR DISCARD without MULTI"), nil
	}

	// Clear the queue
	txManager.Discard(ctx.Conn)

	// Clear the watched keys
	// Note: Skipping UnwatchAll to avoid deadlock
	// The watched keys will be cleaned up when the connection closes

	// Exit MULTI state
	ctx.Conn.SetInMulti(false)

	return command.NewStatusReply("OK"), nil
}

// WATCH marks keys to watch for conditional execution
func watchCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'WATCH' command"), nil
	}

	// Cannot WATCH inside MULTI
	if ctx.Conn.IsInMulti() {
		return command.NewErrorReplyStr("ERR WATCH inside MULTI is not allowed"), nil
	}

	// Add keys to watch list
	txManager.Watch(ctx.Conn, ctx.Args...)

	return command.NewStatusReply("OK"), nil
}

// UNWATCH clears all watched keys
func unwatchCmd(ctx *command.Context) (*command.Reply, error) {
	// Clear all watched keys for this connection
	txManager.UnwatchAll(ctx.Conn)

	return command.NewStatusReply("OK"), nil
}
