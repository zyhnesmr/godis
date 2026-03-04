// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"fmt"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
)

// RegisterObjectCommands registers all object commands
func RegisterObjectCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "OBJECT",
		Handler:    objectCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   2,
		LastKey:    2,
		Categories: []string{command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "MEMORY",
		Handler:    memoryCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   2,
		LastKey:    2,
		Categories: []string{command.CatGeneric},
	})
}

// OBJECT subcommand implementation
// OBJECT ENCODING key - returns the internal encoding of the key
// OBJECT IDLETIME key - returns the idle time in seconds
// OBJECT REFCOUNT key - returns the reference count (always 1 for us)
// OBJECT HELP - returns help text
func objectCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, fmt.Errorf("wrong number of arguments for 'object' command")
	}

	subcmd := ctx.Args[0]

	switch subcmd {
	case "ENCODING":
		if len(ctx.Args) != 2 {
			return nil, fmt.Errorf("wrong number of arguments for 'object encoding' command")
		}
		return objectEncoding(ctx)

	case "IDLETIME":
		if len(ctx.Args) != 2 {
			return nil, fmt.Errorf("wrong number of arguments for 'object idletime' command")
		}
		return objectIdleTime(ctx)

	case "REFCOUNT":
		if len(ctx.Args) != 2 {
			return nil, fmt.Errorf("wrong number of arguments for 'object refcount' command")
		}
		return objectRefCount(ctx)

	case "HELP":
		return command.NewBulkStringReply("OBJECT <subcommand> <key> [args]\n" +
			"Subcommands:\n" +
			"ENCODING  Return internal encoding of the key\n" +
			"IDLETIME  Return the idle time in seconds\n" +
			"REFCOUNT  Return the reference count"), nil

	default:
		return nil, fmt.Errorf("unknown OBJECT subcommand '%s'", subcmd)
	}
}

func objectEncoding(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewBulkStringReply("(none)"), nil
	}

	encoding := getEncoding(obj)
	return command.NewBulkStringReply(encoding), nil
}

func objectIdleTime(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[1]

	_, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(-1), nil
	}

	// Since we don't track idle time, return 0
	return command.NewIntegerReply(0), nil
}

func objectRefCount(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[1]

	_, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	// Reference count is always 1 for our implementation
	return command.NewIntegerReply(1), nil
}

func getEncoding(obj *database.Object) string {
	switch obj.Type {
	case database.ObjTypeString:
		// Check if it's an integer
		if _, ok := obj.Ptr.(int64); ok {
			return "int"
		}
		return "embstr"
	case database.ObjTypeHash:
		return "hashtable"
	case database.ObjTypeList:
		return "linkedlist"
	case database.ObjTypeSet:
		return "hashtable"
	case database.ObjTypeZSet:
		return "skiplist"
	case database.ObjTypeStream:
		return "stream"
	default:
		return "unknown"
	}
}

// MEMORY command implementation
// MEMORY USAGE key [SAMPLES count] - returns memory usage in bytes
func memoryCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, fmt.Errorf("wrong number of arguments for 'memory' command")
	}

	subcmd := strings.ToUpper(ctx.Args[0])

	switch subcmd {
	case "USAGE":
		if len(ctx.Args) < 2 {
			return nil, fmt.Errorf("wrong number of arguments for 'memory usage' command")
		}
		return memoryUsage(ctx)

	case "HELP":
		return command.NewBulkStringReply("MEMORY <subcommand> <key> [args]\n" +
			"Subcommands:\n" +
			"USAGE  Return memory usage in bytes"), nil

	default:
		return nil, fmt.Errorf("unknown MEMORY subcommand '%s'", subcmd)
	}
}

func memoryUsage(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[1]

	// Parse optional SAMPLES option
	// Format: MEMORY USAGE key [SAMPLES count]
	// We ignore samples for now and just return the estimate

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	// Estimate memory usage
	usage := estimateMemoryUsage(obj)
	return command.NewIntegerReply(int64(usage)), nil
}

func estimateMemoryUsage(obj *database.Object) int {
	// Base overhead for object
	baseSize := 32 // Redis object header

	switch obj.Type {
	case database.ObjTypeString:
		if val, ok := obj.Ptr.(string); ok {
			return baseSize + len(val)
		}
		return baseSize

	case database.ObjTypeHash:
		// Estimate hash size
		return baseSize + estimateHashSize(obj)

	case database.ObjTypeList:
		return baseSize + estimateListSize(obj)

	case database.ObjTypeSet:
		return baseSize + estimateSetSize(obj)

	case database.ObjTypeZSet:
		return baseSize + estimateZSetSize(obj)

	case database.ObjTypeStream:
		return baseSize + estimateStreamSize(obj)

	default:
		return baseSize
	}
}

func estimateHashSize(obj *database.Object) int {
	// Import hash package to access internals
	// This is a rough estimate
	return 128 // Base hash table overhead
}

func estimateListSize(obj *database.Object) int {
	return 64 // Base list overhead
}

func estimateSetSize(obj *database.Object) int {
	return 128 // Base set overhead
}

func estimateZSetSize(obj *database.Object) int {
	return 128 // Base zset overhead
}

func estimateStreamSize(obj *database.Object) int {
	return 256 // Base stream overhead
}
