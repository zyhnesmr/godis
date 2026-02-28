// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	hllpkg "github.com/zyhnesmr/godis/internal/datastruct/hyperloglog"
)

// RegisterHyperLogLogCommands registers all HyperLogLog commands
func RegisterHyperLogLogCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "PFADD",
		Handler:    pfaddCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHyperLogLog},
	})

	disp.Register(&command.Command{
		Name:       "PFCOUNT",
		Handler:    pfcountCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatHyperLogLog},
	})

	disp.Register(&command.Command{
		Name:       "PFMERGE",
		Handler:    pfmergeCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatHyperLogLog},
	})
}

// PFADD key element [element ...]
func pfaddCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	elements := ctx.Args[1:]

	// Get or create HyperLogLog
	var hll *hllpkg.HyperLogLog
	obj, ok := ctx.DB.Get(key)
	if ok {
		if obj.Type != database.ObjTypeString {
			return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
		}
		// Try to decode from string
		hll = hllpkg.NewHyperLogLogFromBytes([]byte(obj.String()))
	} else {
		hll = hllpkg.NewHyperLogLog()
	}

	// Add all elements
	modified := false
	for _, elem := range elements {
		if hll.Add(elem) {
			modified = true
		}
	}

	// Store the updated HyperLogLog
	ctx.DB.Set(key, database.NewStringObject(string(hll.Bytes())))

	// Return 1 if the HyperLogLog was modified, 0 otherwise
	if modified {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// PFCOUNT key [key ...]
func pfcountCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	if len(ctx.Args) == 1 {
		// Single key - just count it
		key := ctx.Args[0]
		obj, ok := ctx.DB.Get(key)
		if !ok {
			return command.NewIntegerReply(0), nil
		}

		if obj.Type != database.ObjTypeString {
			return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
		}

		hll := hllpkg.NewHyperLogLogFromBytes([]byte(obj.String()))
		return command.NewIntegerReply(hll.Count()), nil
	}

	// Multiple keys - merge and count
	merged := hllpkg.NewHyperLogLog()

	for _, key := range ctx.Args {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeString {
			return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
		}

		hll := hllpkg.NewHyperLogLogFromBytes([]byte(obj.String()))
		merged.Merge(hll)
	}

	return command.NewIntegerReply(merged.Count()), nil
}

// PFMERGE destkey sourcekey [sourcekey ...]
func pfmergeCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	destKey := ctx.Args[0]
	srcKeys := ctx.Args[1:]

	// Check if destKey exists and is a valid HyperLogLog
	var merged *hllpkg.HyperLogLog
	obj, ok := ctx.DB.Get(destKey)
	if ok {
		if obj.Type != database.ObjTypeString {
			return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
		}
		merged = hllpkg.NewHyperLogLogFromBytes([]byte(obj.String()))
	} else {
		merged = hllpkg.NewHyperLogLog()
	}

	// Merge all source keys
	for _, key := range srcKeys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeString {
			return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
		}

		hll := hllpkg.NewHyperLogLogFromBytes([]byte(obj.String()))
		merged.Merge(hll)
	}

	// Store the merged HyperLogLog
	ctx.DB.Set(destKey, database.NewStringObject(string(merged.Bytes())))

	return command.NewStatusReply("OK"), nil
}
