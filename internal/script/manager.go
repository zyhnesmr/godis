// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package script

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"

	"github.com/yuin/gopher-lua"
	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
)

// ScriptManager manages Lua scripts
type ScriptManager struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script content
}

// NewScriptManager creates a new ScriptManager
func NewScriptManager() *ScriptManager {
	return &ScriptManager{
		scripts: make(map[string]string),
	}
}

// Load loads a script and returns its SHA1 hash
func (sm *ScriptManager) Load(script string) string {
	hash := SHA1(script)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.scripts[hash] = script

	return hash
}

// Exists checks if a script with the given SHA1 hash exists
func (sm *ScriptManager) Exists(sha string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, exists := sm.scripts[sha]
	return exists
}

// Get retrieves a script by its SHA1 hash
func (sm *ScriptManager) Get(sha string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	script, exists := sm.scripts[sha]
	return script, exists
}

// Flush removes all scripts from the cache
func (sm *ScriptManager) Flush() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	count := len(sm.scripts)
	sm.scripts = make(map[string]string)
	return count
}

// GetCount returns the number of cached scripts
func (sm *ScriptManager) GetCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.scripts)
}

// GetAll returns all script SHA1 hashes
func (sm *ScriptManager) GetAll() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	hashes := make([]string, 0, len(sm.scripts))
	for hash := range sm.scripts {
		hashes = append(hashes, hash)
	}
	return hashes
}

// SHA1 calculates the SHA1 hash of a script
func SHA1(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// LuaContext holds context for script execution
type LuaContext struct {
	L           *lua.LState
	DB          *database.DB
	Conn        interface{} // *net.Conn
	NumReplies  int
	Keys        []string // Keys accessed by the script
	Flags       []string
	ConvertedTo map[interface{}]interface{} // Conversion tracking for DEBUG
}

// NewLuaContext creates a new Lua execution context
func NewLuaContext() *LuaContext {
	return &LuaContext{
		ConvertedTo: make(map[interface{}]interface{}),
	}
}

// ExecuteScript executes a Lua script
func (sm *ScriptManager) ExecuteScript(script string, numKeys int, keys []string, args []string, ctx *command.Context) (*command.Reply, error) {
	// Create new Lua state
	L := lua.NewState()
	defer L.Close()

	// Create context
	luaCtx := NewLuaContext()
	luaCtx.L = L
	luaCtx.DB = ctx.DB
	luaCtx.Conn = ctx.Conn

	// Register Redis API functions
	registerRedisAPI(L, luaCtx)

	// Push KEYS array (must be set before script runs)
	keysTbl := L.NewTable()
	for i, key := range keys {
		L.RawSetInt(keysTbl, i+1, lua.LString(key))
	}
	L.SetGlobal("KEYS", keysTbl)

	// Push ARGV array (must be set before script runs)
	argsTbl := L.NewTable()
	for i, arg := range args {
		L.RawSetInt(argsTbl, i+1, lua.LString(arg))
	}
	L.SetGlobal("ARGV", argsTbl)

	// Load and execute the script
	if err := L.DoString(script); err != nil {
		return nil, fmt.Errorf("Error compiling script: %s", err.Error())
	}

	// Get return value from script
	// After DoString, the return value is on top of the stack
	ret := L.Get(-1)
	if ret == lua.LNil {
		return command.NewNilReply(), nil
	}

	// Convert Lua return value to Redis reply
	return convertLuaValueToReply(ret, luaCtx)
}

// convertLuaValueToReply converts a Lua value to a Redis reply
func convertLuaValueToReply(value lua.LValue, ctx *LuaContext) (*command.Reply, error) {
	switch v := value.(type) {
	case *lua.LString:
		return command.NewBulkStringReply(v.String()), nil
	case lua.LNumber:
		// Check if it's actually an integer
		f := float64(v)
		if f == float64(int64(f)) {
			return command.NewIntegerReply(int64(f)), nil
		}
		return command.NewBulkStringReply(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case lua.LBool:
		if bool(v) {
			return command.NewIntegerReply(1), nil
		}
		return command.NewIntegerReply(0), nil
	case *lua.LNilType:
		return command.NewNilReply(), nil
	case *lua.LTable:
		// Table - convert to array
		return convertTableToReply(v, ctx)
	default:
		// Check for nil
		if value == lua.LNil {
			return command.NewNilReply(), nil
		}
		// Unknown type, convert to string
		return command.NewBulkStringReply(fmt.Sprintf("%v", v)), nil
	}
}

// convertTableToReply converts a Lua table to a Redis reply
func convertTableToReply(table *lua.LTable, ctx *LuaContext) (*command.Reply, error) {
	items := make([]*command.Reply, 0)

	// Try to iterate as array
	for i := uint64(1); ; i++ {
		val := ctx.L.RawGetInt(table, int(i))
		if val == lua.LNil {
			break
		}
		reply, err := convertLuaValueToReply(val, ctx)
		if err != nil {
			return nil, err
		}
		items = append(items, reply)
	}

	return command.NewArrayReply(items), nil
}

// registerRedisAPI registers Redis API functions for Lua scripts
func registerRedisAPI(L *lua.LState, ctx *LuaContext) {
	// Create redis table
	redisTbl := L.NewTable()

	// Register redis.call
	L.SetField(redisTbl, "call", L.NewFunction(redisCall(ctx)))

	// Register redis.pcall
	L.SetField(redisTbl, "pcall", L.NewFunction(redisPCall(ctx)))

	// Register redis.error_reply
	L.SetField(redisTbl, "error_reply", L.NewFunction(redisErrorReply(ctx)))

	// Register redis.status_reply
	L.SetField(redisTbl, "status_reply", L.NewFunction(redisStatusReply(ctx)))

	// Register redis.log
	L.SetField(redisTbl, "log", L.NewFunction(redisLog(ctx)))

	// Set global "redis" table
	L.SetGlobal("redis", redisTbl)
}

// redis.call executes a Redis command and returns the result
func redisCall(ctx *LuaContext) lua.LGFunction {
	return func(L *lua.LState) int {
		// Get command name as string
		cmdName := L.CheckString(1)

		// Get number of arguments
		n := L.GetTop()
		args := make([]string, 0, n-1)
		for i := 2; i <= n; i++ {
			arg := L.CheckString(i)
			args = append(args, arg)
		}

		// Execute command through database
		result := executeCommand(ctx.DB, cmdName, args, ctx)

		// Push result to Lua stack
		pushLuaValue(L, result)
		return 1
	}
}

// redis.pcall executes a Redis command and returns the result or error
func redisPCall(ctx *LuaContext) lua.LGFunction {
	return func(L *lua.LState) int {
		// For now, same as redis.call - in production this would catch errors
		return redisCall(ctx)(L)
	}
}

// redis.error_reply sets an error reply for the script
func redisErrorReply(ctx *LuaContext) lua.LGFunction {
	return func(L *lua.LState) int {
		msg := L.CheckString(1)
		// Set error state (simplified - would need proper error handling in production)
		L.Push(lua.LString(msg))
		return 1
	}
}

// redis.status_reply sets a status reply for the script
func redisStatusReply(ctx *LuaContext) lua.LGFunction {
	return func(L *lua.LState) int {
		msg := L.CheckString(1)
		// Set status state (simplified)
		L.Push(lua.LString(msg))
		return 1
	}
}

// redis.log writes to the log
func redisLog(ctx *LuaContext) lua.LGFunction {
	return func(L *lua.LState) int {
		level := L.CheckString(1)
		msg := L.CheckString(2)
		// Log the message (simplified - would use proper logger in production)
		fmt.Printf("[LUA LOG %s] %s\n", level, msg)
		return 0
	}
}

// executeCommand executes a Redis command and returns the result
func executeCommand(db *database.DB, cmdName string, args []string, luaCtx *LuaContext) interface{} {
	luaCtx.NumReplies++

	// Simple command implementations using DB methods
	switch cmdName {
	case "SET":
		if len(args) >= 2 {
			db.Set(args[0], database.NewStringObject(args[1]))
		}
		return lua.LString("OK")
	case "GET":
		if len(args) >= 1 {
			if obj, ok := db.Get(args[0]); ok {
				return lua.LString(obj.String())
			}
		}
		return lua.LNil
	case "DEL":
		if len(args) >= 1 {
			return lua.LNumber(db.Delete(args...))
		}
		return lua.LNumber(0)
	case "EXISTS":
		if len(args) >= 1 {
			return lua.LNumber(db.Exists(args...))
		}
		return lua.LNumber(0)
	case "KEYS":
		// Return all keys matching pattern
		pattern := "*"
		if len(args) >= 1 {
			pattern = args[0]
		}
		keys := db.Keys(pattern)
		tbl := luaCtx.L.NewTable()
		for i, key := range keys {
			luaCtx.L.RawSetInt(tbl, i+1, lua.LString(key))
		}
		return tbl
	case "TTL":
		if len(args) >= 1 {
			return lua.LNumber(db.TTL(args[0]))
		}
		return lua.LNumber(-1)
	case "EXPIRE":
		if len(args) >= 2 {
			seconds, _ := strconv.Atoi(args[1])
			if db.Expire(args[0], seconds) {
				return lua.LNumber(1)
			}
		}
		return lua.LNumber(0)
	case "TYPE":
		if len(args) >= 1 {
			return lua.LString(db.Type(args[0]))
		}
		return lua.LString("none")
	case "INCR":
		return doIncr(db, args[0], 1, luaCtx)
	case "DECR":
		return doIncr(db, args[0], -1, luaCtx)
	case "INCRBY":
		if len(args) >= 2 {
			delta, _ := strconv.Atoi(args[1])
			return doIncr(db, args[0], int64(delta), luaCtx)
		}
		return lua.LNil
	}

	// Unknown command - return nil
	return lua.LNil
}

// doIncr increments a key's value by delta
func doIncr(db *database.DB, key string, delta int64, luaCtx *LuaContext) lua.LValue {
	if key == "" {
		return lua.LNil
	}

	obj, ok := db.Get(key)
	if !ok {
		// Key doesn't exist, create it with delta value
		db.Set(key, database.NewStringObject(strconv.FormatInt(delta, 10)))
		return lua.LNumber(delta)
	}

	// Try to parse as integer
	strVal := obj.String()
	val, err := strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		return lua.LNil // Not an integer
	}

	newVal := val + delta
	db.Set(key, database.NewStringObject(strconv.FormatInt(newVal, 10)))
	return lua.LNumber(newVal)
}

// pushLuaValue pushes a Go value onto the Lua stack
func pushLuaValue(L *lua.LState, value interface{}) {
	// First check if it's already a Lua value
	if lv, ok := value.(lua.LValue); ok {
		L.Push(lv)
		return
	}

	// Otherwise convert Go type to Lua value
	switch v := value.(type) {
	case string:
		L.Push(lua.LString(v))
	case int64:
		L.Push(lua.LNumber(v))
	case int:
		L.Push(lua.LNumber(v))
	case float64:
		L.Push(lua.LNumber(v))
	case bool:
		if v {
			L.Push(lua.LTrue)
		} else {
			L.Push(lua.LFalse)
		}
	case nil:
		L.Push(lua.LNil)
	default:
		L.Push(lua.LString(fmt.Sprintf("%v", v)))
	}
}

// LValueToString converts a Lua value to string
func LValueToString(lv lua.LValue) string {
	switch v := lv.(type) {
	case *lua.LString:
		return v.String()
	case lua.LNumber:
		return strconv.FormatFloat(float64(v), 'f', -1, 64)
	case lua.LBool:
		if bool(v) {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}
