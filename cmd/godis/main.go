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

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/command/commands"
	"github.com/zyhnesmr/godis/internal/config"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/net"
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
	log.SetLevelString(cfg.LogLevel)

	log.Info("Godis %s starting...", Version)
	addr := fmt.Sprintf("%s:%d", cfg.Bind, cfg.Port)
	log.Info("PID: %d", os.Getpid())
	log.Info("Listening on %s", addr)

	// Initialize database selector
	dbSelector := database.NewDBSelector(int(cfg.Databases))

	// Create command dispatcher
	dispatcher := command.NewDispatcher(dbSelector)

	// Register all commands
	registerCommands(dispatcher)

	// Create server
	srv := net.NewServer(cfg.Bind, int(cfg.Port), dispatcher)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		srv.Stop()
	case err := <-errChan:
		log.Error("Server error: %v", err)
		cancel()
		srv.Stop()
	}

	log.Info("Godis shutdown complete")
}

func registerCommands(disp *command.Dispatcher) {
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

	log.Info("Registered %d commands", len(disp.Commands()))
}
