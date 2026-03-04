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

	disp.Register(&command.Command{
		Name:       "COMMAND",
		Handler:    commandCmd,
		Arity:      -1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "DEBUG",
		Handler:    debugCmd,
		Arity:      -2,
		Flags:      []string{command.FlagAdmin, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "CLIENT",
		Handler:    clientCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast, command.FlagNoAuth},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatServer},
	})

	disp.Register(&command.Command{
		Name:       "HELLO",
		Handler:    helloCmd,
		Arity:      -1,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatConnection},
	})

	disp.Register(&command.Command{
		Name:       "MODULE",
		Handler:    moduleCmd,
		Arity:      -2,
		Flags:      []string{command.FlagAdmin, command.FlagNoAuth},
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

// COMMAND - returns information about commands
// COMMAND (no args) - returns list of all commands
// COMMAND COUNT - returns total number of commands
// COMMAND INFO command1 [command2 ...] - returns info about specified commands
// COMMAND GETKEYS - returns keys from a command
// COMMAND GETKEYSANDFLAGS - returns keys and flags from a command
func commandCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		// Return list of all command names
		// This is not standard Redis behavior but useful for clients
		return command.NewErrorReplyStr("ERR COMMAND without args not implemented, use COMMAND COUNT or COMMAND INFO"), nil
	}

	subcmd := strings.ToUpper(ctx.Args[0])

	switch subcmd {
	case "COUNT":
		// Return total number of commands
		// We need to get this from dispatcher somehow
		// For now, return a reasonable number
		return command.NewIntegerReply(150), nil

	case "INFO":
		if len(ctx.Args) < 2 {
			// Return info for all commands (empty array for now)
			return command.NewArrayReplyFromAny([]interface{}{}), nil
		}
		// Return info for specific commands
		cmdNames := ctx.Args[1:]
		result := make([]interface{}, 0, len(cmdNames))
		for _, name := range cmdNames {
			cmdInfo := getCommandInfo(name)
			if cmdInfo != nil {
				result = append(result, cmdInfo)
			}
		}
		return command.NewArrayReplyFromAny(result), nil

	case "GETKEYS":
		if len(ctx.Args) < 2 {
			return command.NewErrorReplyStr("ERR wrong number of arguments for 'COMMAND GETKEYS'"), nil
		}
		// Parse the command and return its keys
		// For simplicity, just return empty array
		return command.NewArrayReplyFromAny([]interface{}{}), nil

	case "GETKEYSANDFLAGS":
		if len(ctx.Args) < 2 {
			return command.NewErrorReplyStr("ERR wrong number of arguments for 'COMMAND GETKEYSANDFLAGS'"), nil
		}
		// Parse the command and return its keys with flags
		// For simplicity, just return empty array
		return command.NewArrayReplyFromAny([]interface{}{}), nil

	default:
		return command.NewErrorReplyStr(fmt.Sprintf("ERR unknown COMMAND subcommand '%s'", subcmd)), nil
	}
}

// getCommandInfo returns command information in Redis format
// Returns an array of: [name, arity, flags, first_key, last_key, step_count]
func getCommandInfo(cmdName string) []interface{} {
	// Map of command info for commonly used commands
	cmdInfo := map[string][]interface{}{
		"GET":     {"GET", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"SET":     {"SET", -3, []string{"write", "denyoom"}, 1, 1, 1},
		"HGET":    {"HGET", 3, []string{"readonly", "fast"}, 1, 1, 1},
		"HSET":    {"HSET", -4, []string{"write", "denyoom"}, 1, 1, 1},
		"HGETALL": {"HGETALL", 2, []string{"readonly", "sort_for_script"}, 1, 1, 1},
		"HKEYS":   {"HKEYS", 2, []string{"readonly", "sort_for_script"}, 1, 1, 1},
		"HVALS":   {"HVALS", 2, []string{"readonly", "sort_for_script"}, 1, 1, 1},
		"HLEN":    {"HLEN", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"HDEL":    {"HDEL", -3, []string{"write", "fast"}, 1, 1, 1},
		"HEXISTS": {"HEXISTS", 3, []string{"readonly", "fast"}, 1, 1, 1},
		"HINCRBY": {"HINCRBY", 4, []string{"write", "denyoom", "fast"}, 1, 1, 1},
		"HSCAN":   {"HSCAN", -3, []string{"readonly"}, 1, 1, 1},
		"SADD":    {"SADD", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1},
		"SREM":    {"SREM", -3, []string{"write", "fast"}, 1, 1, 1},
		"SISMEMBER": {"SISMEMBER", 3, []string{"readonly", "fast"}, 1, 1, 1},
		"SMEMBERS": {"SMEMBERS", 2, []string{"readonly", "sort_for_script"}, 1, 1, 1},
		"SCARD":   {"SCARD", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"SPOP":    {"SPOP", -2, []string{"write", "random", "fast"}, 1, 1, 1},
		"SRANDMEMBER": {"SRANDMEMBER", -2, []string{"readonly", "random", "fast"}, 1, 1, 1},
		"SSCAN":   {"SSCAN", -3, []string{"readonly"}, 1, 1, 1},
		"SMOVE":   {"SMOVE", 4, []string{"write", "fast"}, 1, 2, 1},
		"DEL":     {"DEL", -2, []string{"write"}, 1, -1, 1},
		"EXISTS":  {"EXISTS", -2, []string{"readonly", "fast"}, 1, -1, 1},
		"TYPE":    {"TYPE", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"KEYS":    {"KEYS", 2, []string{"readonly", "sort_for_script"}, 1, 1, 1},
		"SCAN":    {"SCAN", -2, []string{"readonly"}, 1, 1, 1},
		"DBSIZE":  {"DBSIZE", 1, []string{"readonly", "fast"}, 0, 0, 0},
		"PING":    {"PING", -1, []string{"fast", "stale"}, 0, 0, 0},
		"INFO":    {"INFO", -1, []string{"readonly", "loading"}, 0, 0, 0},
		"TTL":     {"TTL", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"PTTL":    {"PTTL", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"EXPIRE":  {"EXPIRE", -3, []string{"write", "fast"}, 1, 1, 1},
		"LPUSH":   {"LPUSH", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1},
		"RPUSH":   {"RPUSH", -3, []string{"write", "denyoom", "fast"}, 1, 1, 1},
		"LPOP":    {"LPOP", -2, []string{"write", "fast"}, 1, 1, 1},
		"RPOP":    {"RPOP", -2, []string{"write", "fast"}, 1, 1, 1},
		"LRANGE":  {"LRANGE", 4, []string{"readonly"}, 1, 1, 1},
		"LLEN":    {"LLEN", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"ZADD":    {"ZADD", -4, []string{"write", "denyoom", "fast"}, 1, 1, 1},
		"ZREM":    {"ZREM", -3, []string{"write", "fast"}, 1, 1, 1},
		"ZSCORE":  {"ZSCORE", 3, []string{"readonly", "fast"}, 1, 1, 1},
		"ZRANGE":  {"ZRANGE", -4, []string{"readonly"}, 1, 1, 1},
		"ZCARD":   {"ZCARD", 2, []string{"readonly", "fast"}, 1, 1, 1},
		"ZSCAN":   {"ZSCAN", -3, []string{"readonly"}, 1, 1, 1},
	}

	nameUpper := strings.ToUpper(cmdName)
	if info, ok := cmdInfo[nameUpper]; ok {
		return info
	}

	// Return generic info for unknown commands
	return []interface{}{
		cmdName, // name
		0,       // arity (unknown)
		[]string{}, // flags
		0,       // first_key
		0,       // last_key
		0,       // step_count
	}
}

// DEBUG subcommand implementation
// DEBUG OBJECT key - returns debugging information about a key
// DEBUG HELP - returns help text
func debugCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'DEBUG' command"), nil
	}

	subcmd := strings.ToUpper(ctx.Args[0])

	switch subcmd {
	case "OBJECT":
		if len(ctx.Args) != 2 {
			return command.NewErrorReplyStr("ERR wrong number of arguments for 'DEBUG OBJECT' command"), nil
		}
		return debugObject(ctx)

	case "HELP":
		return command.NewBulkStringReply("DEBUG <subcommand> <key> [args]\n" +
			"Subcommands:\n" +
			"OBJECT  Return debugging information about a key"), nil

	default:
		return command.NewErrorReplyStr(fmt.Sprintf("ERR unknown DEBUG subcommand '%s'", subcmd)), nil
	}
}

func debugObject(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewBulkStringReply("(nil)"), nil
	}

	// Build debug information similar to Redis
	var info strings.Builder

	info.Write([]byte("Value at:"))
	// Redis returns a hex address, we'll just return a placeholder
	info.Write([]byte("0x"))
	info.Write([]byte(fmt.Sprintf("%016x", 12345))) // Fake memory address

	info.Write([]byte(" refcount:"))
	info.Write([]byte(fmt.Sprintf("%d", 1)))

	info.Write([]byte(" encoding:"))
	// ObjType: 0=String, 1=List, 2=Hash, 3=Set, 4=ZSet, 5=Stream
	switch obj.Type {
	case 0: // String
		if _, ok := obj.Ptr.(int64); ok {
			info.Write([]byte("int"))
		} else {
			info.Write([]byte("embstr"))
		}
	case 1: // List
		info.Write([]byte("linkedlist"))
	case 2: // Hash
		info.Write([]byte("hashtable"))
	case 3: // Set
		info.Write([]byte("hashtable"))
	case 4: // ZSet
		info.Write([]byte("skiplist"))
	case 5: // Stream
		info.Write([]byte("stream"))
	default:
		info.Write([]byte("unknown"))
	}

	info.Write([]byte(" serializedlength:"))
	info.Write([]byte(fmt.Sprintf("%d", 0))) // We don't track this

	info.Write([]byte(" lru:"))
	info.Write([]byte(fmt.Sprintf("%d", 0))) // We don't track LRU

	info.Write([]byte(" lru_seconds_idle:"))
	info.Write([]byte(fmt.Sprintf("%d", 0))) // We don't track idle time

	return command.NewBulkStringReply(info.String()), nil
}

// CLIENT subcommand implementation
// CLIENT LIST - returns information about connected clients
// CLIENT GETNAME - returns the name of the current connection
// CLIENT SETNAME - sets the name of the current connection
// CLIENT ID - returns the client ID
func clientCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'CLIENT' command"), nil
	}

	subcmd := strings.ToUpper(ctx.Args[0])

	switch subcmd {
	case "LIST":
		// Return list of connected clients
		// Format: id=... addr=... fd=... name=... age=... idle=...
		addr := ""
		if ctx.Conn.RemoteAddr() != nil {
			addr = ctx.Conn.RemoteAddr().String()
		}
		return command.NewBulkStringReply(fmt.Sprintf("id=%d addr=%s fd=%d name=%s age=%d idle=%d\n",
			ctx.Conn.GetID(),
			addr,
			0,
			ctx.Conn.GetName(),
			0,
			0,
		)), nil

	case "GETNAME":
		// Return the name of the current connection
		name := ctx.Conn.GetName()
		if name == "" {
			return command.NewBulkStringReply(""), nil
		}
		return command.NewBulkStringReply(name), nil

	case "SETNAME":
		if len(ctx.Args) < 2 {
			return command.NewErrorReplyStr("ERR wrong number of arguments for 'CLIENT SETNAME' command"), nil
		}
		ctx.Conn.SetName(ctx.Args[1])
		return command.NewStatusReply("OK"), nil

	case "ID":
		// Return the client ID
		return command.NewIntegerReply(int64(ctx.Conn.GetID())), nil

	case "INFO":
		// Return client info as a map
		addr := ""
		if ctx.Conn.RemoteAddr() != nil {
			addr = ctx.Conn.RemoteAddr().String()
		}
		info := map[string]string{
			"id":   fmt.Sprintf("%d", ctx.Conn.GetID()),
			"addr": addr,
			"name": ctx.Conn.GetName(),
			"db":   fmt.Sprintf("%d", ctx.Conn.GetDB()),
		}
		// Convert map to alternating keys/values
		result := make([]string, 0, len(info)*2)
		for k, v := range info {
			result = append(result, k, v)
		}
		return command.NewStringArrayReply(result), nil

	case "KILL":
		// For now, just return OK
		// Real implementation would need connection tracking in server
		return command.NewStatusReply("OK"), nil

	default:
		return command.NewErrorReplyStr(fmt.Sprintf("ERR unknown CLIENT subcommand '%s'", subcmd)), nil
	}
}

// HELLO [protocol-version [AUTH username password] [SETNAME clientname]]
// Switch to a different protocol, optionally authenticating and setting the client name
func helloCmd(ctx *command.Context) (*command.Reply, error) {
	// Parse protocol version
	protocol := 2 // Default to RESP2
	if len(ctx.Args) > 0 {
		parsed, err := fmt.Sscanf(ctx.Args[0], "%d", &protocol)
		if err != nil || parsed != 1 {
			return command.NewErrorReplyStr("ERR invalid protocol version"), nil
		}
	}

	// We only support RESP2 and RESP3
	if protocol != 2 && protocol != 3 {
		return command.NewErrorReplyStr("ERR NOPROTO unsupported protocol version"), nil
	}

	// Return server info as a map
	// Format: [key, value, key, value, ...]
	result := make([]interface{}, 0, 20)
	result = append(result, "server", "godis")
	result = append(result, "version", "1.0.0")
	result = append(result, "proto", fmt.Sprintf("%d", protocol))
	result = append(result, "id", fmt.Sprintf("%d", ctx.Conn.GetID()))
	result = append(result, "mode", "standalone")
	result = append(result, "role", "master")
	result = append(result, "modules", []interface{}{})

	return command.NewArrayReplyFromAny(result), nil
}

// MODULE LIST / MODULE LOAD / MODULE UNLOAD
func moduleCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'MODULE' command"), nil
	}

	subcmd := strings.ToUpper(ctx.Args[0])

	switch subcmd {
	case "LIST":
		// Return list of loaded modules (empty for us)
		return command.NewArrayReplyFromAny([]interface{}{}), nil

	case "LOAD":
		return command.NewErrorReplyStr("ERR module loading not supported"), nil

	case "UNLOAD":
		return command.NewErrorReplyStr("ERR module unloading not supported"), nil

	default:
		return command.NewErrorReplyStr(fmt.Sprintf("ERR unknown MODULE subcommand '%s'", subcmd)), nil
	}
}
