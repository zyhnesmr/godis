// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	scriptpkg "github.com/zyhnesmr/godis/internal/script"
)

// SetScriptManager sets the global script manager (used during initialization)
var scriptManager *scriptpkg.ScriptManager

// SetScriptManager sets the global script manager
func SetScriptManager(sm *scriptpkg.ScriptManager) {
	scriptManager = sm
}

// GetScriptManager returns the global script manager
func GetScriptManager() *scriptpkg.ScriptManager {
	return scriptManager
}

// RegisterScriptCommands registers all script commands
func RegisterScriptCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "EVAL",
		Handler:    evalCmd,
		Arity:      -3,
		Flags:      []string{command.FlagNoScript, command.FlagSkipMonitor, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatScript},
	})

	disp.Register(&command.Command{
		Name:       "EVALSHA",
		Handler:    evalshaCmd,
		Arity:      -3,
		Flags:      []string{command.FlagNoScript, command.FlagSkipMonitor, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatScript},
	})

	disp.Register(&command.Command{
		Name:       "SCRIPT",
		Handler:    scriptCmd,
		Arity:      -2,
		Flags:      []string{command.FlagNoScript, command.FlagSkipSlowlog},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatScript},
	})
}

// EVAL script numkeys key [key ...] arg [arg ...]
func evalCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	script := ctx.Args[0]
	numKeys, err := strconv.Atoi(ctx.Args[1])
	if err != nil || numKeys < 0 {
		return nil, errors.New("Number of keys can't be negative")
	}

	argsStart := 2 + numKeys
	argsEnd := len(ctx.Args)

	if argsStart > argsEnd && len(ctx.Args) > argsStart {
		return nil, errors.New("Number of keys can't be greater than number of args")
	}

	var keys []string
	var args []string

	if numKeys > 0 {
		if argsStart > len(ctx.Args) {
			return nil, errors.New("Number of keys can't be greater than number of args")
		}
		keys = ctx.Args[2 : 2+numKeys]
	}

	if argsStart < len(ctx.Args) {
		args = ctx.Args[argsStart:]
	}

	// Execute script
	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	return scriptManager.ExecuteScript(script, numKeys, keys, args, ctx)
}

// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
func evalshaCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	sha1 := ctx.Args[0]
	numKeys, err := strconv.Atoi(ctx.Args[1])
	if err != nil || numKeys < 0 {
		return nil, errors.New("Number of keys can't be negative")
	}

	argsStart := 2 + numKeys
	argsEnd := len(ctx.Args)

	if argsStart > argsEnd && len(ctx.Args) > argsStart {
		return nil, errors.New("Number of keys can't be greater than number of args")
	}

	var keys []string
	var args []string

	if numKeys > 0 {
		if argsStart > len(ctx.Args) {
			return nil, errors.New("Number of keys can't be greater than number of args")
		}
		keys = ctx.Args[2 : 2+numKeys]
	}

	if argsStart < len(ctx.Args) {
		args = ctx.Args[argsStart:]
	}

	// Check if script exists
	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	script, exists := scriptManager.Get(sha1)
	if !exists {
		return nil, errors.New("NOSCRIPT No matching script found")
	}

	return scriptManager.ExecuteScript(script, numKeys, keys, args, ctx)
}

// SCRIPT LOAD script
func scriptLoadCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	// Args[0] is "LOAD", the script content starts from Args[1]
	script := strings.Join(ctx.Args[1:], " ")

	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	sha1 := scriptManager.Load(script)

	return command.NewBulkStringReply(sha1), nil
}

// SCRIPT EXISTS sha1 [sha1 ...]
func scriptExistsCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	// Args[0] is "EXISTS", the SHA1 hashes start from Args[1]
	sha1Args := ctx.Args[1:]
	results := make([]interface{}, len(sha1Args))
	for i, sha1 := range sha1Args {
		if scriptManager.Exists(sha1) {
			results[i] = int64(1)
		} else {
			results[i] = int64(0)
		}
	}

	return command.NewArrayReplyFromAny(results), nil
}

// SCRIPT FLUSH
func scriptFlushCmd(ctx *command.Context) (*command.Reply, error) {
	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	count := scriptManager.Flush()

	return command.NewStatusReply(fmt.Sprintf("OK. Counted: %d", count)), nil
}

// SCRIPT KILL
func scriptKillCmd(ctx *command.Context) (*command.Reply, error) {
	// In production, this would kill running scripts
	// For now, just return OK as scripts are synchronous
	return command.NewStatusReply("OK"), nil
}

// SCRIPT SHOW sha1
func scriptShowCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	sha1 := ctx.Args[1]

	if scriptManager == nil {
		return nil, errors.New("Script manager not initialized")
	}

	script, exists := scriptManager.Get(sha1)
	if !exists {
		return nil, errors.New("NOSCRIPT No matching script found")
	}

	// Format: multi bulk reply with each line as a bulk string
	lines := strings.Split(script, "\n")
	results := make([]*command.Reply, len(lines))

	for i, line := range lines {
		results[i] = command.NewBulkStringReply(line)
	}

	return command.NewArrayReply(results), nil
}

// SCRIPT subcommand handler
func scriptCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	subcommand := strings.ToUpper(ctx.Args[0])

	switch subcommand {
	case "LOAD":
		if len(ctx.Args) < 2 {
			return nil, errors.New("wrong number of arguments")
		}
		return scriptLoadCmd(ctx)
	case "EXISTS":
		if len(ctx.Args) < 2 {
			return nil, errors.New("wrong number of arguments")
		}
		return scriptExistsCmd(ctx)
	case "FLUSH":
		return scriptFlushCmd(ctx)
	case "KILL":
		return scriptKillCmd(ctx)
	case "SHOW":
		if len(ctx.Args) < 2 {
			return nil, errors.New("wrong number of arguments")
		}
		return scriptShowCmd(ctx)
	default:
		return nil, errors.New("Unknown SCRIPT subcommand")
	}
}
