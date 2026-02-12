// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/config"
)

// RegisterServerCommands registers all server commands
func RegisterServerCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:             "PING",
		Handler:          pingCmd,
		Arity:            -1, // At least 1, but OptionalFirstArg allows 0
		Flags:            []string{command.FlagFast, command.FlagStale},
		FirstKey:         0,
		LastKey:          0,
		Categories:       []string{command.CatConnection, command.CatServer},
		OptionalFirstArg: true, // Allow PING without arguments
	})

	disp.Register(&command.Command{
		Name:       "ECHO",
		Handler:    echoCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "QUIT",
		Handler:    quitCmd,
		Arity:      -1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatConnection},
	})

	disp.Register(&command.Command{
		Name:       "SELECT",
		Handler:    selectCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatConnection},
	})

	disp.Register(&command.Command{
		Name:       "AUTH",
		Handler:    authCmd,
		Arity:      -1,
		Flags:      []string{command.FlagNoAuth, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatConnection},
	})

	disp.Register(&command.Command{
		Name:       "INFO",
		Handler:    infoCmd,
		Arity:      -1,
		Flags:      []string{command.FlagReadOnly, command.FlagLoading},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "DBSIZE",
		Handler:    dbsizeCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey, command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "TIME",
		Handler:    timeCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})
}

var startTime = time.Now()

// PING [message]
func pingCmd(ctx *command.Context) (*command.Reply, error) {
	// Handle 0 or 1 arguments
	if len(ctx.Args) == 0 {
		return command.NewStatusReply("PONG"), nil
	}
	if len(ctx.Args) == 1 {
		return command.NewBulkStringReply(ctx.Args[0]), nil
	}
	return command.NewBulkStringReply(ctx.Args[0]), nil
}

// ECHO message
func echoCmd(ctx *command.Context) (*command.Reply, error) {
	return command.NewBulkStringReply(ctx.Args[0]), nil
}

// QUIT
func quitCmd(ctx *command.Context) (*command.Reply, error) {
	return command.NewStatusReply("OK"), nil
}

// SELECT index
func selectCmd(ctx *command.Context) (*command.Reply, error) {
	index, err := parseDBIndex(ctx.Args[0])
	if err != nil {
		return command.NewErrorReply(err), nil
	}

	ctx.Conn.SetDB(index)
	return command.NewStatusReply("OK"), nil
}

func parseDBIndex(s string) (int, error) {
	var index int
	if _, err := fmt.Sscanf(s, "%d", &index); err != nil {
		return 0, fmt.Errorf("invalid DB index")
	}

	cfg := config.Instance()
	if index < 0 || index >= cfg.Databases {
		return 0, fmt.Errorf("DB index out of range")
	}

	return index, nil
}

// AUTH [password]
func authCmd(ctx *command.Context) (*command.Reply, error) {
	// No password configured - just return OK
	return command.NewStatusReply("OK"), nil
}

// INFO [section]
func infoCmd(ctx *command.Context) (*command.Reply, error) {
	section := "default"
	if len(ctx.Args) > 0 {
		section = strings.ToLower(ctx.Args[0])
	}

	var info string

	switch section {
	case "default", "all":
		info = buildDefaultInfo()
	case "server":
		info = buildServerInfo()
	case "memory":
		info = buildMemoryInfo()
	case "stats":
		info = buildStatsInfo()
	case "replication":
		info = buildReplicationInfo()
	case "persistence":
		info = buildPersistenceInfo()
	default:
		info = buildDefaultInfo()
	}

	return command.NewBulkStringReply(info), nil
}

func buildDefaultInfo() string {
	var b strings.Builder

	b.WriteString("# Server\r\n")
	b.WriteString(fmt.Sprintf("godis_version:1.0.0\r\n"))
	b.WriteString(fmt.Sprintf("os:%s\r\n", runtime.GOOS))
	b.WriteString(fmt.Sprintf("arch:%s\r\n", runtime.GOARCH))
	b.WriteString(fmt.Sprintf("process_id:%d\r\n", 1))
	b.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", int64(time.Since(startTime).Seconds())))
	b.WriteString(fmt.Sprintf("uptime_in_days:%d\r\n", int64(time.Since(startTime).Seconds()/86400)))

	b.WriteString("\r\n# Clients\r\n")
	b.WriteString(fmt.Sprintf("connected_clients:%d\r\n", 1))
	b.WriteString(fmt.Sprintf("blocked_clients:0\r\n"))

	b.WriteString("\r\n# Memory\r\n")
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b.WriteString(fmt.Sprintf("used_memory:%d\r\n", m.Alloc))
	b.WriteString(fmt.Sprintf("used_memory_human:%s\r\n", formatBytes(m.Alloc)))

	b.WriteString("\r\n# Persistence\r\n")
	b.WriteString("loading:0\r\n")

	b.WriteString("\r\n# Stats\r\n")
	b.WriteString("total_connections_received:1\r\n")
	b.WriteString("total_commands_processed:1\r\n")

	b.WriteString("\r\n# Replication\r\n")
	b.WriteString("role:master\r\n")

	return b.String()
}

func buildServerInfo() string {
	var b strings.Builder

	b.WriteString("# Server\r\n")
	b.WriteString(fmt.Sprintf("godis_version:1.0.0\r\n"))
	b.WriteString(fmt.Sprintf("os:%s\r\n", runtime.GOOS))
	b.WriteString(fmt.Sprintf("arch:%s\r\n", runtime.GOARCH))
	b.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", int64(time.Since(startTime).Seconds())))

	return b.String()
}

func buildMemoryInfo() string {
	var b strings.Builder
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	b.WriteString("# Memory\r\n")
	b.WriteString(fmt.Sprintf("used_memory:%d\r\n", m.Alloc))
	b.WriteString(fmt.Sprintf("used_memory_human:%s\r\n", formatBytes(m.Alloc)))
	b.WriteString(fmt.Sprintf("used_memory_rss:%d\r\n", m.Sys))
	b.WriteString(fmt.Sprintf("used_memory_peak:%d\r\n", m.Alloc))

	return b.String()
}

func buildStatsInfo() string {
	var b strings.Builder

	b.WriteString("# Stats\r\n")
	b.WriteString("total_connections_received:1\r\n")
	b.WriteString("total_commands_processed:1\r\n")
	b.WriteString("instantaneous_ops_per_sec:0\r\n")

	return b.String()
}

func buildReplicationInfo() string {
	var b strings.Builder

	b.WriteString("# Replication\r\n")
	b.WriteString("role:master\r\n")
	b.WriteString("connected_slaves:0\r\n")

	return b.String()
}

func buildPersistenceInfo() string {
	var b strings.Builder

	b.WriteString("# Persistence\r\n")
	b.WriteString("loading:0\r\n")

	return b.String()
}

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%c", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// DBSIZE
func dbsizeCmd(ctx *command.Context) (*command.Reply, error) {
	return command.NewIntegerReply(int64(ctx.DB.DBSize())), nil
}

// TIME
func timeCmd(ctx *command.Context) (*command.Reply, error) {
	now := time.Now()
	unix := now.Unix()
	micro := now.Nanosecond() / 1000

	result := make([]string, 2)
	result[0] = fmt.Sprintf("%d", unix)
	result[1] = fmt.Sprintf("%d", micro)

	return command.NewStringArrayReply(result), nil
}
