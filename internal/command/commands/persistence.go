// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/persistence/aof"
	"github.com/zyhnesmr/godis/internal/persistence/rdb"
)

var (
	rdbManager *rdb.RDB
	dbSelector *database.DBSelector
)

// SetRDBManager sets the global RDB manager
func SetRDBManager(mgr *rdb.RDB) {
	rdbManager = mgr
}

// SetDBSelectorForPersistence sets the database selector for persistence
func SetDBSelectorForPersistence(selector *database.DBSelector) {
	dbSelector = selector
}

// saveInProgress is used to prevent concurrent saves
var saveInProgress int32 // 0 = not in progress, 1 = in progress

// RegisterPersistenceCommands registers all persistence commands
func RegisterPersistenceCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "SAVE",
		Handler:    saveCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPersistence},
	})

	disp.Register(&command.Command{
		Name:       "BGSAVE",
		Handler:    bgsaveCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPersistence},
	})

	disp.Register(&command.Command{
		Name:       "LASTSAVE",
		Handler:    lastsaveCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPersistence},
	})

	// Register AOF commands
	aof.RegisterAOFCommands(disp)
}

// SAVE synchronously saves the dataset to disk
func saveCmd(ctx *command.Context) (*command.Reply, error) {
	// Check if another save is in progress
	if atomic.LoadInt32(&saveInProgress) == 1 {
		return command.NewErrorReplyStr("ERR Background save already in progress"), nil
	}

	atomic.StoreInt32(&saveInProgress, 1)
	defer atomic.StoreInt32(&saveInProgress, 0)

	startTime := time.Now()

	// Collect all databases
	dbs := make([]*database.DB, dbSelector.Count())
	for i := 0; i < dbSelector.Count(); i++ {
		db, err := dbSelector.GetDB(i)
		if err != nil {
			return command.NewErrorReply(err), nil
		}
		dbs[i] = db
	}

	// Perform save
	if err := rdbManager.Save(dbs); err != nil {
		return command.NewErrorReply(err), nil
	}

	duration := time.Since(startTime)
	return command.NewStatusReply(fmt.Sprintf("OK. Duration: %s", duration)), nil
}

// BGSAVE asynchronously saves the dataset to disk
func bgsaveCmd(ctx *command.Context) (*command.Reply, error) {
	// Check if another save is in progress
	if !atomic.CompareAndSwapInt32(&saveInProgress, 0, 1) {
		return command.NewErrorReplyStr("ERR Background save already in progress"), nil
	}

	// Run save in background
	go func() {
		defer atomic.StoreInt32(&saveInProgress, 0)

		// Collect all databases
		dbs := make([]*database.DB, dbSelector.Count())
		for i := 0; i < dbSelector.Count(); i++ {
			db, err := dbSelector.GetDB(i)
			if err != nil {
				return
			}
			dbs[i] = db
		}

		// Perform save
		if err := rdbManager.Save(dbs); err != nil {
			// Log error - in real implementation would use proper logging
			fmt.Fprintf(os.Stderr, "BGSAVE failed: %v\n", err)
		}
	}()

	return command.NewStatusReply("Background saving started"), nil
}

// LASTSAVE returns the Unix time of the last successful save
func lastsaveCmd(ctx *command.Context) (*command.Reply, error) {
	// Get file info
	info, err := os.Stat(rdbManager.GetFilename())
	if err != nil {
		if os.IsNotExist(err) {
			// No save has been performed yet
			return command.NewIntegerReply(0), nil
		}
		return command.NewErrorReply(err), nil
	}

	// Return Unix timestamp
	return command.NewIntegerReply(info.ModTime().Unix()), nil
}

// LogToAOF logs a command to AOF if enabled
func LogToAOF(db int, cmdName string, args []string) error {
	return aof.LogCommandForAOF(db, cmdName, args)
}

// IsAOFEnabled returns true if AOF is enabled
func IsAOFEnabled() bool {
	return aof.IsAOFEnabled()
}

// ShouldRewriteAOF returns true if AOF rewrite should be triggered
func ShouldRewriteAOF() bool {
	return aof.ShouldRewriteAOF()
}

// RewriteAOFNow performs an AOF rewrite
func RewriteAOFNow(dbs []*database.DB) error {
	return aof.RewriteAOF(dbs)
}

// GetAOFManager returns the AOF manager
func GetAOFManager() *aof.AOF {
	return aof.GetAOFManager()
}