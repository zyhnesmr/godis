// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package log

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Level represents the log level
type Level int

const (
	LevelDebug Level = iota
	LevelVerbose
	LevelNotice
	LevelWarning
	LevelError
)

var (
	level     Level = LevelNotice
	output    *log.Logger
	mu        sync.RWMutex
	file      *os.File
	pid       int
	logToFile atomic.Bool
	closed    atomic.Bool
)

func init() {
	output = log.New(os.Stdout, "", 0)
	pid = os.Getpid()
}

// SetLevel sets the log level
func SetLevel(l Level) {
	mu.Lock()
	defer mu.Unlock()
	level = l
}

// SetLevelString sets the log level from string
func SetLevelString(s string) {
	mu.Lock()
	defer mu.Unlock()

	switch s {
	case "debug":
		level = LevelDebug
	case "verbose":
		level = LevelVerbose
	case "notice":
		level = LevelNotice
	case "warning":
		level = LevelWarning
	case "error":
		level = LevelError
	default:
		level = LevelNotice
	}
}

// SetOutput sets the log output
func SetOutput(out *os.File) {
	mu.Lock()
	defer mu.Unlock()

	if file != nil {
		file.Close()
	}

	file = out
	output = log.New(out, "", 0)
	logToFile.Store(out != os.Stdout && out != os.Stderr)
}

// Close closes the log file if open
func Close() {
	if closed.Swap(true) {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if file != nil && file != os.Stdout && file != os.Stderr {
		file.Close()
		file = nil
	}
}

// Debug logs a debug message
func Debug(format string, args ...interface{}) {
	mu.RLock()
	l := level
	mu.RUnlock()

	if l <= LevelDebug {
		logMsg("DEBUG", format, args...)
	}
}

// Verbose logs a verbose message
func Verbose(format string, args ...interface{}) {
	mu.RLock()
	l := level
	mu.RUnlock()

	if l <= LevelVerbose {
		logMsg("VERBOSE", format, args...)
	}
}

// Info logs an info message (notice level)
func Info(format string, args ...interface{}) {
	mu.RLock()
	l := level
	mu.RUnlock()

	if l <= LevelNotice {
		logMsg("NOTICE", format, args...)
	}
}

// Warning logs a warning message
func Warning(format string, args ...interface{}) {
	mu.RLock()
	l := level
	mu.RUnlock()

	if l <= LevelWarning {
		logMsg("WARNING", format, args...)
	}
}

// Warn is an alias for Warning
func Warn(format string, args ...interface{}) {
	Warning(format, args...)
}

// Error logs an error message
func Error(format string, args ...interface{}) {
	mu.RLock()
	l := level
	mu.RUnlock()

	if l <= LevelError {
		logMsg("ERROR", format, args...)
	}
}

// Fatal logs a fatal message and exits
func Fatal(format string, args ...interface{}) {
	logMsg("FATAL", format, args...)
	os.Exit(1)
}

// Printf logs a message at notice level
func Printf(format string, args ...interface{}) {
	Info(format, args...)
}

// Println logs a message at notice level
func Println(args ...interface{}) {
	Info("%s", fmt.Sprint(args...))
}

func logMsg(levelStr, format string, args ...interface{}) {
	now := time.Now()
	timestamp := now.Format("2006-01-02 15:04:05.000")

	msg := fmt.Sprintf(format, args...)
	output.Printf("%s [%d] %s %s\n", timestamp, pid, levelStr, msg)
}

// GetLevel returns the current log level
func GetLevel() Level {
	mu.RLock()
	defer mu.RUnlock()
	return level
}

// IsDebugEnabled returns true if debug logging is enabled
func IsDebugEnabled() bool {
	return level <= LevelDebug
}

// IsVerboseEnabled returns true if verbose logging is enabled
func IsVerboseEnabled() bool {
	return level <= LevelVerbose
}
