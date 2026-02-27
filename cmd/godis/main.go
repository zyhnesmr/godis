// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/command/commands"
	"github.com/zyhnesmr/godis/internal/config"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/eviction"
	"github.com/zyhnesmr/godis/internal/expire"
	"github.com/zyhnesmr/godis/internal/net"
	"github.com/zyhnesmr/godis/internal/pubsub"
	aof2 "github.com/zyhnesmr/godis/internal/persistence/aof"
	rdb2 "github.com/zyhnesmr/godis/internal/persistence/rdb"
	"github.com/zyhnesmr/godis/pkg/log"
)

var (
	Version   = "1.0.0"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	// Load configuration
	cfg := config.Instance()
	cfg.ParseFlags() // Parse command line flags and config file
	log.SetLevelString(cfg.LogLevel)

	log.Info("Godis %s starting...", Version)
	addr := fmt.Sprintf("%s:%d", cfg.Bind, cfg.Port)
	log.Info("PID: %d", os.Getpid())
	log.Info("Listening on %s", addr)

	// Parse eviction policy from config
	evictionPolicy, err := eviction.PolicyFromString(cfg.MaxMemoryPolicy)
	if err != nil {
		log.Warn("Invalid eviction policy '%s', using noeviction: %v", cfg.MaxMemoryPolicy, err)
		evictionPolicy = eviction.PolicyNoEviction
	}

	// Initialize database selector with eviction support
	var dbSelector *database.DBSelector
	if cfg.MaxMemory > 0 && evictionPolicy != eviction.PolicyNoEviction {
		dbSelector = database.NewDBSelectorWithEviction(
			int(cfg.Databases),
			evictionPolicy,
			cfg.MaxMemory,
		)
		log.Info("Eviction: policy=%s maxmemory=%d", evictionPolicy.String(), cfg.MaxMemory)
	} else {
		dbSelector = database.NewDBSelector(int(cfg.Databases))
		if cfg.MaxMemory > 0 {
			log.Info("Max memory limit: %d bytes (noeviction)", cfg.MaxMemory)
		}
	}

	// Initialize expire manager
	expireMgr := expire.NewManager(func(db int, key string) {
		// Callback when a key expires
		if dbInst, err := dbSelector.GetDB(db); err == nil {
			dbInst.Delete(key)
			log.Debug("Expired key: db=%d key=%s", db, key)
		}
	})

	// Start expire scheduler
	expireScheduler := expire.NewScheduler(expireMgr)
	expireScheduler.Start()
	log.Info("Expire scheduler started")

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start eviction checker (if eviction is enabled)
	evictionMgr := dbSelector.GetEvictionManager()
	if evictionMgr.IsEnabled() {
		go runEvictionChecker(ctx, dbSelector)
		log.Info("Eviction checker started")
	}

	// Create command dispatcher
	dispatcher := command.NewDispatcher(dbSelector)

	// Register all commands
	aofMgr := registerCommands(dispatcher, dbSelector, cfg)

	// Set AOF logger (will check if enabled internally)
	dispatcher.SetAOFLogger(aofMgr)

	// Load data from persistence files
	// If AOF file exists, load AOF (it has more recent data)
	// Otherwise load RDB
	if aofMgr != nil && aofMgr.FileExists() {
		log.Info("Loading AOF file on startup")
		loadAOFOnStartup(dbSelector, cfg, aofMgr, func(db int, cmdName string, args []string) error {
			dbInst, err := dbSelector.GetDB(db)
			if err != nil {
				return err
			}

			cmd, ok := dispatcher.Get(cmdName)
			if !ok {
				return nil
			}

			ctx := &command.Context{
				DB:      dbInst,
				CmdName: cmdName,
				Args:    args,
			}

			_, err = cmd.Handler(ctx)
			return err
		})
	} else {
		// Load RDB file if exists
		loadRDBOnStartup(dbSelector, cfg)
	}

	// Create server
	srv := net.NewServer(cfg.Bind, int(cfg.Port), dispatcher)

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for signal or error
	select {
	case <-sigChan:
		log.Info("Received shutdown signal")
		cancel()
		expireScheduler.Stop()
		srv.Stop()
	case err := <-errChan:
		log.Error("Server error: %v", err)
		cancel()
		expireScheduler.Stop()
		srv.Stop()
	}

	log.Info("Godis shutdown complete")
}

// runEvictionChecker periodically checks and performs eviction
func runEvictionChecker(ctx context.Context, dbSelector *database.DBSelector) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if dbSelector.ShouldEvict() {
				evicted, err := dbSelector.ProcessEviction(0)
				if err != nil {
					log.Error("Eviction failed: %v", err)
				} else if evicted > 0 {
					log.Debug("Evicted %d keys", evicted)
				}
			}
		}
	}
}

func registerCommands(disp *command.Dispatcher, dbSelector *database.DBSelector, cfg *config.Config) *aof2.AOF {
	// Initialize pubsub manager
	mgr := pubsub.NewManager()
	commands.SetPubSubManager(mgr)

	// Set transaction manager to DBSelector for dirty key tracking
	txManager := disp.GetTxManager()
	disp.GetDB().SetTransactionManager(txManager)

	// Register transaction commands with tx manager
	commands.SetTxManager(txManager)
	commands.RegisterTransactionCommands(disp)

	// Initialize RDB manager
	rdbMgr := rdb2.NewRDB(cfg.Dir, cfg.RdbFilename)
	commands.SetRDBManager(rdbMgr)
	commands.SetDBSelectorForPersistence(dbSelector)

	// Initialize AOF manager
	aofMgr := aof2.NewAOF(cfg.Dir, cfg.AppendFilename, cfg)
	aof2.SetAOFManager(aofMgr)
	aof2.SetDBSelectorForAOF(dbSelector)

	// Set command handler for AOF replay
	aof2.SetCommandHandler(func(db int, cmdName string, args []string) error {
		// Get database
		dbInst, err := dbSelector.GetDB(db)
		if err != nil {
			return err
		}

		// Get command
		cmd, ok := disp.Get(cmdName)
		if !ok {
			return nil // Skip unknown commands
		}

		// Create context and execute
		ctx := &command.Context{
			DB:      dbInst,
			CmdName: cmdName,
			Args:    args,
		}

		_, err = cmd.Handler(ctx)
		return err
	})

	// Enable AOF if configured
	if strings.ToLower(cfg.AppendOnly) == "yes" {
		if err := aofMgr.Enable(); err != nil {
			log.Warn("Failed to enable AOF: %v", err)
		} else {
			log.Info("AOF enabled: %s", aofMgr.GetFilename())
		}
	}

	// Register server commands
	commands.RegisterServerCommands(disp)

	// Register key commands
	commands.RegisterKeyCommands(disp)

	// Register string commands
	commands.RegisterStringCommands(disp)

	// Register hash commands
	commands.RegisterHashCommands(disp)

	// Register list commands
	commands.RegisterListCommands(disp)

	// Register set commands
	commands.RegisterSetCommands(disp)

	// Register zset commands
	commands.RegisterZSetCommands(disp)

	// Register pubsub commands
	commands.RegisterPubSubCommands(disp)

	// Register persistence commands (including AOF)
	commands.RegisterPersistenceCommands(disp)

	// Register stream commands
	commands.RegisterStreamCommands(disp)

	log.Info("Registered %d commands", len(disp.Commands()))

	return aofMgr
}

// loadRDBOnStartup loads the RDB file on startup if it exists
func loadRDBOnStartup(dbSelector *database.DBSelector, cfg *config.Config) {
	rdbMgr := rdb2.NewRDB(cfg.Dir, cfg.RdbFilename)
	if !rdbMgr.FileExists() {
		log.Info("No RDB file found, starting with empty database")
		return
	}

	log.Info("Loading RDB file: %s", rdbMgr.GetFilename())
	dbs := make([]*database.DB, dbSelector.Count())
	for i := 0; i < dbSelector.Count(); i++ {
		db, err := dbSelector.GetDB(i)
		if err != nil {
			log.Error("Failed to get DB %d: %v", i, err)
			continue
		}
		dbs[i] = db
	}
	if err := rdbMgr.Load(dbs); err != nil {
		log.Warn("Failed to load RDB file: %v", err)
	} else {
		log.Info("RDB file loaded successfully")
	}
}

// loadAOFOnStartup loads the AOF file on startup if it exists
func loadAOFOnStartup(dbSelector *database.DBSelector, cfg *config.Config, aofMgr *aof2.AOF, handler aof2.CommandHandler) {
	if !aofMgr.FileExists() {
		log.Info("No AOF file found")
		return
	}

	// If AOF is enabled, load it instead of RDB (AOF has more recent data)
	log.Info("Loading AOF file: %s", aofMgr.GetFilename())
	dbs := make([]*database.DB, dbSelector.Count())
	for i := 0; i < dbSelector.Count(); i++ {
		db, err := dbSelector.GetDB(i)
		if err != nil {
			log.Error("Failed to get DB %d: %v", i, err)
			continue
		}
		dbs[i] = db
	}
	if err := aofMgr.Load(dbs, handler); err != nil {
		log.Warn("Failed to load AOF file: %v", err)
	} else {
		log.Info("AOF file loaded successfully")
	}
}
