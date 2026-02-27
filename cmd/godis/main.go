// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
	registerCommands(dispatcher)

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

func registerCommands(disp *command.Dispatcher) {
	// Initialize pubsub manager
	mgr := pubsub.NewManager()
	commands.SetPubSubManager(mgr)

	// Set transaction manager to DBSelector for dirty key tracking
	txManager := disp.GetTxManager()
	disp.GetDB().SetTransactionManager(txManager)

	// Register transaction commands with tx manager
	commands.SetTxManager(txManager)
	commands.RegisterTransactionCommands(disp)

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

	log.Info("Registered %d commands", len(disp.Commands()))
}
