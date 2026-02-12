// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
)

// RegisterKeyCommands registers all key management commands
func RegisterKeyCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "DEL",
		Handler:    delCmd,
		Arity:      -2,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "EXISTS",
		Handler:    existsCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "TYPE",
		Handler:    typeCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "KEYS",
		Handler:    keysCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "RANDOMKEY",
		Handler:    randomkeyCmd,
		Arity:      1,
		Flags:      []string{command.FlagReadOnly, command.FlagRandom},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "RENAME",
		Handler:    renameCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    2,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "RENAMENX",
		Handler:    renamenxCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    2,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "EXPIRE",
		Handler:    expireCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "EXPIREAT",
		Handler:    expireatCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "TTL",
		Handler:    ttlCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "PTTL",
		Handler:    pttlCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "PERSIST",
		Handler:    persistCmd,
		Arity:      2,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "FLUSHDB",
		Handler:    flushdbCmd,
		Arity:      -1,
		Flags:      []string{command.FlagWrite},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "FLUSHALL",
		Handler:    flushallCmd,
		Arity:      -1,
		Flags:      []string{command.FlagWrite},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey},
	})

	disp.Register(&command.Command{
		Name:       "SCAN",
		Handler:    scanCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagRandom},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatKey},
	})
}

// DEL key [key ...]
func delCmd(ctx *command.Context) (*command.Reply, error) {
	count := ctx.DB.Delete(ctx.Args...)
	return command.NewIntegerReply(int64(count)), nil
}

// EXISTS key [key ...]
func existsCmd(ctx *command.Context) (*command.Reply, error) {
	count := ctx.DB.Exists(ctx.Args...)
	return command.NewIntegerReply(int64(count)), nil
}

// TYPE key
func typeCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	return command.NewBulkStringReply(ctx.DB.Type(key)), nil
}

// KEYS pattern
func keysCmd(ctx *command.Context) (*command.Reply, error) {
	pattern := ctx.Args[0]
	keys := ctx.DB.Keys(pattern)
	return command.NewStringArrayReply(keys), nil
}

// RANDOMKEY
func randomkeyCmd(ctx *command.Context) (*command.Reply, error) {
	key, ok := ctx.DB.RandomKey()
	if !ok {
		return command.NewNilReply(), nil
	}
	return command.NewBulkStringReply(key), nil
}

// RENAME key newkey
func renameCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}
	key := ctx.Args[0]
	newKey := ctx.Args[1]

	err := ctx.DB.Rename(key, newKey)
	if err != nil {
		return command.NewErrorReplyStr("ERR no such key"), nil
	}

	return command.NewStatusReply("OK"), nil
}

// RENAMENX key newkey
func renamenxCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}
	key := ctx.Args[0]
	newKey := ctx.Args[1]

	renamed, err := ctx.DB.RenameNX(key, newKey)
	if err != nil {
		return command.NewErrorReplyStr("ERR no such key"), nil
	}

	if renamed {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// EXPIRE key seconds
func expireCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}
	key := ctx.Args[0]
	seconds, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return command.NewErrorReplyStr("ERR value is not an integer or out of range"), nil
	}

	ok := ctx.DB.Expire(key, seconds)
	if ok {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// EXPIREAT key timestamp
func expireatCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}
	key := ctx.Args[0]
	timestamp, err := strconv.ParseInt(ctx.Args[1], 10, 64)
	if err != nil {
		return command.NewErrorReplyStr("ERR value is not an integer or out of range"), nil
	}

	ok := ctx.DB.ExpireAt(key, timestamp)
	if ok {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// TTL key
func ttlCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	ttl := ctx.DB.TTL(key)
	return command.NewIntegerReply(ttl), nil
}

// PTTL key
func pttlCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	pttl := ctx.DB.PTTL(key)
	return command.NewIntegerReply(pttl), nil
}

// PERSIST key
func persistCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	ok := ctx.DB.Persist(key)
	if ok {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// FLUSHDB [ASYNC | SYNC]
func flushdbCmd(ctx *command.Context) (*command.Reply, error) {
	async := false
	if len(ctx.Args) > 0 {
		option := strings.ToUpper(ctx.Args[0])
		if option == "ASYNC" {
			async = true
		} else if option != "SYNC" {
			return command.NewErrorReplyStr("ERR syntax error"), nil
		}
	}

	if async {
		// Async flush - run in background
		go ctx.DB.FlushDB()
		return command.NewStatusReply("OK"), nil
	}

	ctx.DB.FlushDB()
	return command.NewStatusReply("OK"), nil
}

// FLUSHALL [ASYNC | SYNC]
func flushallCmd(ctx *command.Context) (*command.Reply, error) {
	async := false
	if len(ctx.Args) > 0 {
		option := strings.ToUpper(ctx.Args[0])
		if option == "ASYNC" {
			async = true
		} else if option != "SYNC" {
			return command.NewErrorReplyStr("ERR syntax error"), nil
		}
	}

	// Flush all databases
	// Note: This needs access to the DB selector
	// For now, we only flush the current DB
	if async {
		go ctx.DB.FlushDB()
		return command.NewStatusReply("OK"), nil
	}

	ctx.DB.FlushDB()
	return command.NewStatusReply("OK"), nil
}

// SCAN cursor [MATCH pattern] [COUNT count]
func scanCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		return command.NewErrorReplyStr("ERR syntax error"), nil
	}

	cursor, err := strconv.Atoi(ctx.Args[0])
	if err != nil || cursor < 0 {
		return command.NewErrorReplyStr("ERR invalid cursor"), nil
	}

	pattern := "*"
	count := 10

	// Parse options
	for i := 1; i < len(ctx.Args); i++ {
		arg := strings.ToUpper(ctx.Args[i])
		switch arg {
		case "MATCH":
			if i+1 >= len(ctx.Args) {
				return command.NewErrorReplyStr("ERR syntax error"), nil
			}
			pattern = ctx.Args[i+1]
			i++
		case "COUNT":
			if i+1 >= len(ctx.Args) {
				return command.NewErrorReplyStr("ERR syntax error"), nil
			}
			count, err = strconv.Atoi(ctx.Args[i+1])
			if err != nil || count <= 0 {
				return command.NewErrorReplyStr("ERR syntax error"), nil
			}
			i++
		}
	}

	// Scan keys
	newCursor, keys := ctx.DB.Scan(cursor, count, pattern)

	// Build response array with cursor and keys
	arr := make([]*command.Reply, 2)
	arr[0] = command.NewBulkStringReply(strconv.Itoa(newCursor))
	arr[1] = command.NewStringArrayReply(keys)

	return command.NewArrayReply(arr), nil
}
