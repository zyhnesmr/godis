// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aof

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
)

var (
	aofManager     *AOF
	dbSelector     *database.DBSelector
	commandHandler CommandHandler
)

// SetAOFManager sets the global AOF manager
func SetAOFManager(mgr *AOF) {
	aofManager = mgr
}

// SetDBSelectorForAOF sets the database selector for AOF
func SetDBSelectorForAOF(selector *database.DBSelector) {
	dbSelector = selector
}

// SetCommandHandler sets the command handler for AOF replay
func SetCommandHandler(handler CommandHandler) {
	commandHandler = handler
}

// rewriteInProgress is used to prevent concurrent rewrites
var rewriteInProgress atomic.Bool

// RegisterAOFCommands registers all AOF commands
func RegisterAOFCommands(disp interface{}) {
	type registerer interface {
		Register(*command.Command)
	}

	// Try to register commands
	if r, ok := disp.(registerer); ok {
		r.Register(&command.Command{
			Name:       "APPENDONLY",
			Handler:    appendonlyCmd,
			Arity:      -2,
			Flags:      []string{command.FlagAdmin, command.FlagWrite},
			FirstKey:   0,
			LastKey:    0,
			Categories: []string{command.CatPersistence},
		})

		r.Register(&command.Command{
			Name:       "BGREWRITEAOF",
			Handler:    bgrewriteaofCmd,
			Arity:      1,
			Flags:      []string{command.FlagAdmin, command.FlagWrite},
			FirstKey:   0,
			LastKey:    0,
			Categories: []string{command.CatPersistence},
		})
	}
}

// APPENDONLY YES|NO
func appendonlyCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return command.NewErrorReplyStr("ERR wrong number of arguments"), nil
	}

	value := ctx.Args[0]
	if aofManager == nil {
		return command.NewErrorReplyStr("ERR AOF not initialized"), nil
	}

	switch value {
	case "yes", "YES", "on", "ON", "1":
		if err := aofManager.Enable(); err != nil {
			return command.NewErrorReply(err), nil
		}
		return command.NewStatusReply("OK"), nil
	case "no", "NO", "off", "OFF", "0":
		if err := aofManager.Disable(); err != nil {
			return command.NewErrorReply(err), nil
		}
		return command.NewStatusReply("OK"), nil
	default:
		return command.NewErrorReplyStr("ERR argument must be 'yes' or 'no'"), nil
	}
}

// BGREWRITEAOF
func bgrewriteaofCmd(ctx *command.Context) (*command.Reply, error) {
	if aofManager == nil {
		return command.NewErrorReplyStr("ERR AOF not initialized"), nil
	}

	if !rewriteInProgress.CompareAndSwap(false, true) {
		return command.NewErrorReplyStr("ERR Background append only file rewriting already in progress"), nil
	}

	// Check if AOF is enabled
	if !aofManager.IsEnabled() {
		rewriteInProgress.Store(false)
		return command.NewStatusReply("Background append only file rewriting started"), nil
	}

	// Run rewrite in background
	go func() {
		defer rewriteInProgress.Store(false)

		// Collect all databases
		dbs := make([]*database.DB, dbSelector.Count())
		for i := 0; i < dbSelector.Count(); i++ {
			db, err := dbSelector.GetDB(i)
			if err != nil {
				fmt.Fprintf(os.Stderr, "BGREWRITEAOF failed: %v\n", err)
				return
			}
			dbs[i] = db
		}

		startTime := time.Now()
		if err := aofManager.Rewrite(dbs); err != nil {
			fmt.Fprintf(os.Stderr, "BGREWRITEAOF failed: %v\n", err)
		} else {
			duration := time.Since(startTime)
			fmt.Fprintf(os.Stderr, "BGREWRITEAOF completed in %s\n", duration)
		}
	}()

	return command.NewStatusReply("Background append only file rewriting started"), nil
}

// LogCommandForAOF logs a command to AOF if enabled
func LogCommandForAOF(db int, cmdName string, args []string) error {
	if aofManager == nil || !aofManager.IsEnabled() {
		return nil
	}
	return aofManager.LogCommand(db, cmdName, args)
}

// IsAOFEnabled returns true if AOF is enabled
func IsAOFEnabled() bool {
	if aofManager == nil {
		return false
	}
	return aofManager.IsEnabled()
}

// GetAOFManager returns the AOF manager
func GetAOFManager() *AOF {
	return aofManager
}

// ShouldRewriteAOF returns true if AOF rewrite should be triggered
func ShouldRewriteAOF() bool {
	if aofManager == nil {
		return false
	}
	return aofManager.ShouldRewrite()
}

// RewriteAOF performs an AOF rewrite
func RewriteAOF(dbs []*database.DB) error {
	if aofManager == nil {
		return fmt.Errorf("AOF not initialized")
	}
	return aofManager.Rewrite(dbs)
}
