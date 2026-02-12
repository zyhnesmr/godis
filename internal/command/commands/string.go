// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
)

// RegisterStringCommands registers all string commands
func RegisterStringCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "SET",
		Handler:    setCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "GET",
		Handler:    getCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "MGET",
		Handler:    mgetCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "MSET",
		Handler:    msetCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    -1,
		StepCount:  2,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "SETEX",
		Handler:    setexCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "PSETEX",
		Handler:    psetexCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "SETNX",
		Handler:    setnxCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "INCR",
		Handler:    incrCmd,
		Arity:      2,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "INCRBY",
		Handler:    incrbyCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "DECR",
		Handler:    decrCmd,
		Arity:      2,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "DECRBY",
		Handler:    decrbyCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "INCRBYFLOAT",
		Handler:    incrbyfloatCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "APPEND",
		Handler:    appendCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "STRLEN",
		Handler:    strlenCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "GETRANGE",
		Handler:    getrangeCmd,
		Arity:      4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "SETRANGE",
		Handler:    setrangeCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})

	disp.Register(&command.Command{
		Name:       "GETSET",
		Handler:    getsetCmd,
		Arity:      3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString},
	})
}

type Dispatcher interface {
	Register(cmd *command.Command)
}

// SET key value [NX|XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds]
func setCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	value := args[1]

	// Parse options
	nx := false
	xx := false
	get := false
	var exDuration time.Duration
	var exTime int64

	i := 2
	for i < len(args) {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "EX":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			seconds, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("invalid expire time")
			}
			exDuration = time.Duration(seconds) * time.Second
			i++
		case "PX":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			ms, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("invalid expire time")
			}
			exDuration = time.Duration(ms) * time.Millisecond
			i++
		case "EXAT":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			timestamp, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, errors.New("invalid expire time")
			}
			exTime = timestamp
			i++
		case "PXAT":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, errors.New("invalid expire time")
			}
			exTime = ms / 1000
			i++
		default:
			return nil, errors.New("syntax error")
		}
		i++
	}

	// Check for conflicting options
	if nx && xx {
		return nil, errors.New("NX and XX options at the same time")
	}

	// Get old value if GET option is set
	var oldValue string
	if get {
		if obj, ok := ctx.DB.Get(key); ok {
			oldValue = obj.String()
		}
	}

	// Check existence conditions
	if nx && ctx.DB.Exists(key) > 0 {
		if get {
			return command.NewNilReply(), nil
		}
		return command.NewNilReply(), nil
	}
	if xx && ctx.DB.Exists(key) == 0 {
		if get {
			return command.NewNilReply(), nil
		}
		return command.NewNilReply(), nil
	}

	// Set the value
	obj := database.NewStringObject(value)
	ctx.DB.Set(key, obj)

	// Set expiration
	if exDuration > 0 {
		ctx.DB.Expire(key, int(exDuration.Seconds()))
	} else if exTime > 0 {
		ctx.DB.ExpireAt(key, exTime)
	}

	// Return old value if GET was set
	if get {
		if oldValue == "" {
			return command.NewNilReply(), nil
		}
		return command.NewBulkStringReply(oldValue), nil
	}

	return command.NewStatusReply("OK"), nil
}

// GET key
func getCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	return command.NewBulkStringReply(obj.String()), nil
}

// MGET key [key ...]
func mgetCmd(ctx *command.Context) (*command.Reply, error) {
	result := make([]string, len(ctx.Args))

	for i, key := range ctx.Args {
		if obj, ok := ctx.DB.Get(key); ok {
			result[i] = obj.String()
		} else {
			result[i] = "" // nil in RESP
		}
	}

	return command.NewStringArrayReply(result), nil
}

// MSET key value [key value ...]
func msetCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args)%2 != 0 {
		return nil, errors.New("wrong number of arguments")
	}

	for i := 0; i < len(ctx.Args); i += 2 {
		key := ctx.Args[i]
		value := ctx.Args[i+1]
		obj := database.NewStringObject(value)
		ctx.DB.Set(key, obj)
	}

	return command.NewStatusReply("OK"), nil
}

// SETEX key seconds value
func setexCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	seconds, err := strconv.Atoi(ctx.Args[1])
	if err != nil || seconds < 0 {
		return nil, errors.New("invalid expire time")
	}
	value := ctx.Args[2]

	obj := database.NewStringObject(value)
	ctx.DB.Set(key, obj)
	ctx.DB.Expire(key, seconds)

	return command.NewStatusReply("OK"), nil
}

// PSETEX key milliseconds value
func psetexCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	ms, err := strconv.Atoi(ctx.Args[1])
	if err != nil || ms < 0 {
		return nil, errors.New("invalid expire time")
	}
	value := ctx.Args[2]

	obj := database.NewStringObject(value)
	ctx.DB.Set(key, obj)
	ctx.DB.Expire(key, ms/1000)

	return command.NewStatusReply("OK"), nil
}

// SETNX key value
func setnxCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	value := ctx.Args[1]

	obj := database.NewStringObject(value)
	set := ctx.DB.SetNX(key, obj)

	if set {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// INCR key
func incrCmd(ctx *command.Context) (*command.Reply, error) {
	return doIncr(ctx, 1)
}

// INCRBY key delta
func incrbyCmd(ctx *command.Context) (*command.Reply, error) {
	delta, err := strconv.ParseInt(ctx.Args[1], 10, 64)
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}
	return doIncr(ctx, delta)
}

// DECR key
func decrCmd(ctx *command.Context) (*command.Reply, error) {
	return doIncr(ctx, -1)
}

// DECRBY key delta
func decrbyCmd(ctx *command.Context) (*command.Reply, error) {
	delta, err := strconv.ParseInt(ctx.Args[1], 10, 64)
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}
	return doIncr(ctx, -delta)
}

func doIncr(ctx *command.Context, delta int64) (*command.Reply, error) {
	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Create new integer object
		obj = database.NewIntObject(delta)
		ctx.DB.Set(key, obj)
		return command.NewIntegerReply(delta), nil
	}

	// Get current value
	current, ok := obj.Int()
	if !ok {
		return nil, errors.New("value is not an integer")
	}

	// Check for overflow
	newVal := current + delta
	if (delta > 0 && newVal < current) || (delta < 0 && newVal > current) {
		return nil, errors.New("increment would overflow")
	}

	obj = database.NewIntObject(newVal)
	ctx.DB.Set(key, obj)

	return command.NewIntegerReply(newVal), nil
}

// INCRBYFLOAT key delta
func incrbyfloatCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	delta, err := strconv.ParseFloat(ctx.Args[1], 64)
	if err != nil {
		return nil, errors.New("value is not a valid float")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Create new float object
		newVal := strconv.FormatFloat(delta, 'f', -1, 64)
		obj = database.NewStringObject(newVal)
		ctx.DB.Set(key, obj)
		return command.NewBulkStringReply(newVal), nil
	}

	// Get current value
	currentStr := obj.String()
	current, err := strconv.ParseFloat(currentStr, 64)
	if err != nil {
		return nil, errors.New("value is not a valid float")
	}

	// Check for overflow
	newVal := current + delta
	if math.IsInf(newVal, 0) || math.IsNaN(newVal) {
		return nil, errors.New("increment would produce NaN or Infinity")
	}

	newValStr := strconv.FormatFloat(newVal, 'f', -1, 64)
	obj = database.NewStringObject(newValStr)
	ctx.DB.Set(key, obj)

	return command.NewBulkStringReply(newValStr), nil
}

// APPEND key value
func appendCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	value := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		obj = database.NewStringObject(value)
		ctx.DB.Set(key, obj)
		return command.NewIntegerReply(int64(len(value))), nil
	}

	newValue := obj.String() + value
	newObj := database.NewStringObject(newValue)
	ctx.DB.Set(key, newObj)

	return command.NewIntegerReply(int64(len(newValue))), nil
}

// STRLEN key
func strlenCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	return command.NewIntegerReply(int64(len(obj.String()))), nil
}

// GETRANGE key start end
func getrangeCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]

	start, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	end, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewBulkStringReply(""), nil
	}

	s := obj.String()
	runes := []rune(s)
	length := len(runes)

	// Handle negative indices
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	// Clamp indices
	if start >= length {
		return command.NewBulkStringReply(""), nil
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return command.NewBulkStringReply(""), nil
	}

	result := string(runes[start : end+1])
	return command.NewBulkStringReply(result), nil
}

// SETRANGE key offset value
func setrangeCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]

	offset, err := strconv.Atoi(ctx.Args[1])
	if err != nil || offset < 0 {
		return nil, errors.New("offset is out of range")
	}

	value := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	var s string
	if ok {
		s = obj.String()
	}

	// Extend string if needed
	if offset > len(s) {
		padding := strings.Repeat("\x00", offset-len(s))
		s = s + padding + value
	} else {
		runes := []rune(s)
		valueRunes := []rune(value)

		for i, r := range valueRunes {
			if offset+i < len(runes) {
				runes[offset+i] = r
			} else {
				runes = append(runes, r)
			}
		}
		s = string(runes)
	}

	newObj := database.NewStringObject(s)
	ctx.DB.Set(key, newObj)

	return command.NewIntegerReply(int64(len(s))), nil
}

// GETSET key value
func getsetCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]
	value := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)

	newObj := database.NewStringObject(value)
	ctx.DB.Set(key, newObj)

	if !ok {
		return command.NewNilReply(), nil
	}

	return command.NewBulkStringReply(obj.String()), nil
}
