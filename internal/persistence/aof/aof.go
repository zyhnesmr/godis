// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zyhnesmr/godis/internal/config"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// FsyncStrategy defines when to fsync the AOF file
type FsyncStrategy int

const (
	FsyncAlways FsyncStrategy = iota // fsync after every write
	FsyncEverySec                    // fsync every second
	FsyncNo                          // let OS decide
)

// AOF manages AOF persistence
type AOF struct {
	dirname  string
	dbname   string
	cfg      *config.Config
	file     *os.File
	writer   *bufio.Writer
	mu       sync.RWMutex
	enabled  atomic.Bool
	fsyncStr FsyncStrategy

	// Statistics
	lastRewriteTime    time.Time
	currentRewriteSize int64
	baseSize          int64

	// Rewrite state
	rewriteInProgress atomic.Bool

	// Fsync channel
	fsyncChan chan struct{}
	closeChan chan struct{}
}

// NewAOF creates a new AOF manager
func NewAOF(dirname, dbname string, cfg *config.Config) *AOF {
	a := &AOF{
		dirname:  dirname,
		dbname:   dbname,
		cfg:      cfg,
		fsyncStr: parseFsyncStrategy(cfg.AppendFsync),
		fsyncChan: make(chan struct{}, 1),
		closeChan: make(chan struct{}),
	}

	// Check if AOF is enabled
	a.enabled.Store(strings.ToLower(cfg.AppendOnly) == "yes")

	return a
}

// parseFsyncStrategy parses the fsync strategy from config
func parseFsyncStrategy(s string) FsyncStrategy {
	switch strings.ToLower(s) {
	case "always":
		return FsyncAlways
	case "everysec":
		return FsyncEverySec
	case "no":
		return FsyncNo
	default:
		return FsyncEverySec
	}
}

// IsEnabled returns true if AOF is enabled
func (a *AOF) IsEnabled() bool {
	return a.enabled.Load()
}

// Enable enables AOF
func (a *AOF) Enable() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.enabled.Load() {
		return nil
	}

	// Ensure directory exists
	if err := os.MkdirAll(a.dirname, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Open file for appending
	filename := a.GetFilename()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	a.file = file
	a.writer = bufio.NewWriterSize(file, 32*1024) // 32KB buffer
	a.enabled.Store(true)

	// Start fsync goroutine
	go a.fsyncLoop()

	return nil
}

// Disable disables AOF
func (a *AOF) Disable() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled.Load() {
		return nil
	}

	a.enabled.Store(false)

	// Stop fsync loop
	close(a.closeChan)

	// Flush and close
	if a.writer != nil {
		if err := a.writer.Flush(); err != nil {
			return err
		}
	}
	if a.file != nil {
		if err := a.file.Sync(); err != nil {
			return err
		}
		if err := a.file.Close(); err != nil {
			return err
		}
	}

	a.file = nil
	a.writer = nil

	return nil
}

// GetFilename returns the full path to the AOF file
func (a *AOF) GetFilename() string {
	return filepath.Join(a.dirname, a.dbname)
}

// LogCommand logs a command to the AOF file
func (a *AOF) LogCommand(db int, cmdName string, args []string) error {
	if !a.enabled.Load() {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.writer == nil {
		return nil
	}

	// Build the command as RESP array
	// Format: *<count>\r\n$<len>\r\n<cmd>\r\n$<len>\r\n<arg>\r\n...
	builder := resp.NewResponseBuilder()

	// Switch DB command if needed
	// We track the current DB and emit SELECT commands when it changes

	// Write the command array
	totalArgs := 1 + len(args)
	builder.WriteArray(totalArgs)
	builder.WriteBulkStringFromString(cmdName)
	for _, arg := range args {
		builder.WriteBulkStringFromString(arg)
	}

	// Write to buffer
	if _, err := a.writer.Write(builder.Bytes()); err != nil {
		return fmt.Errorf("failed to write to AOF: %w", err)
	}

	// Flush after each command for now (can be optimized later)
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF: %w", err)
	}

	// Fsync based on strategy
	switch a.fsyncStr {
	case FsyncAlways:
		if err := a.fsync(); err != nil {
			return err
		}
	case FsyncEverySec:
		// Signal fsync goroutine
		select {
		case a.fsyncChan <- struct{}{}:
		default:
		}
	case FsyncNo:
		// Let OS handle it
	}

	return nil
}

// LogSelectDB logs a SELECT command to switch database
func (a *AOF) LogSelectDB(db int) error {
	return a.LogCommand(0, "SELECT", []string{strconv.Itoa(db)})
}

// fsync performs an fsync on the file
func (a *AOF) fsync() error {
	if a.file == nil {
		return nil
	}

	// Flush buffer first
	if err := a.writer.Flush(); err != nil {
		return err
	}

	// Fsync file
	return a.file.Sync()
}

// fsyncLoop runs fsync every second if needed
func (a *AOF) fsyncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.closeChan:
			return
		case <-ticker.C:
			a.mu.Lock()
			if a.enabled.Load() && a.writer != nil {
				_ = a.writer.Flush()
				_ = a.file.Sync()
			}
			a.mu.Unlock()
		case <-a.fsyncChan:
			// Triggered fsync
			a.mu.Lock()
			if a.enabled.Load() && a.writer != nil {
				_ = a.writer.Flush()
				_ = a.file.Sync()
			}
			a.mu.Unlock()
		}
	}
}

// Load loads the AOF file and replays commands
func (a *AOF) Load(dbs []*database.DB, handler CommandHandler) error {
	filename := a.GetFilename()
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No AOF file, that's ok
		}
		return fmt.Errorf("failed to open AOF file: %w", err)
	}
	defer file.Close()

	parser := resp.NewParser(bufio.NewReader(file))

	// Current database
	currentDB := 0

	// Parse and replay commands
	for {
		msg, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to parse AOF: %w", err)
		}

		if msg == nil {
			break
		}

		// Should be an array (command)
		if msg.Type != resp.TypeArray {
			continue
		}

		array, _ := msg.Array()
		if len(array) == 0 {
			continue
		}

		// Extract command name and args
		cmdName := ""
		args := []string{}

		for i, item := range array {
			if str, ok := item.String(); ok {
				if i == 0 {
					cmdName = strings.ToUpper(str)
				} else {
					args = append(args, str)
				}
			}
		}

		if cmdName == "" {
			continue
		}

		// Handle SELECT command
		if cmdName == "SELECT" {
			if len(args) > 0 {
				dbNum, err := strconv.Atoi(args[0])
				if err == nil && dbNum >= 0 && dbNum < len(dbs) {
					currentDB = dbNum
				}
			}
			continue
		}

		// Skip non-write commands during replay
		if !isWriteCommand(cmdName) {
			continue
		}

		// Execute command
		if err := handler(currentDB, cmdName, args); err != nil {
			// Log error but continue
			fmt.Fprintf(os.Stderr, "Failed to execute AOF command: %s %v - %v\n", cmdName, args, err)
		}
	}

	return nil
}

// isWriteCommand returns true if the command modifies data
func isWriteCommand(cmdName string) bool {
	writeCommands := []string{
		"SET", "SETNX", "SETEX", "PSETEX", "MSET", "MSETNX", "GETSET", "APPEND", "SETRANGE",
		"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
		"DEL", "UNLINK", "EXPIRE", "EXPIREAT", "PERSIST",
		"RPUSH", "LPUSH", "RPUSHX", "LPUSHX", "LINSERT", "LSET", "LTRIM", "RPOP", "LPOP",
		"SADD", "SREM", "SPOP", "SMOVE", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
		"ZADD", "ZINCRBY", "ZREM", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
		"HSET", "HSETNX", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HDEL",
		"RENAME", "RENAMENX",
		"FLUSHDB", "FLUSHALL",
		"PUBLISH",
	}

	for _, wc := range writeCommands {
		if cmdName == wc {
			return true
		}
	}
	return false
}

// FileSize returns the size of the AOF file in bytes
func (a *AOF) FileSize() (int64, error) {
	filename := a.GetFilename()
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// FileExists checks if the AOF file exists
func (a *AOF) FileExists() bool {
	filename := a.GetFilename()
	_, err := os.Stat(filename)
	return err == nil
}

// Delete removes the AOF file
func (a *AOF) Delete() error {
	filename := a.GetFilename()
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ShouldRewrite returns true if AOF rewrite should be triggered
func (a *AOF) ShouldRewrite() bool {
	if a.rewriteInProgress.Load() {
		return false
	}

	size, err := a.FileSize()
	if err != nil {
		return false
	}

	// Check if file meets minimum size
	minSize := a.cfg.AutoAofRewriteMinSize
	if minSize == 0 {
		minSize = 64 << 20 // Default 64MB
	}
	if size < minSize {
		return false
	}

	// Check growth percentage
	percentage := a.cfg.AutoAofRewritePercentage
	if percentage == 0 {
		percentage = 100
	}

	if a.baseSize == 0 {
		a.baseSize = size
		return false
	}

	growth := (size - a.baseSize) * 100 / a.baseSize
	return int64(growth) >= int64(percentage)
}

// CommandHandler is the interface for executing commands during AOF load
type CommandHandler func(db int, cmdName string, args []string) error

// Close closes the AOF file
func (a *AOF) Close() error {
	return a.Disable()
}
