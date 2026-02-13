// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"strconv"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/hash"
)

// RegisterHashCommands registers all hash commands
func RegisterHashCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "HSET",
		Handler:    hsetCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HGET",
		Handler:    hgetCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HMSET",
		Handler:    hmsetCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HMGET",
		Handler:    hmgetCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HSETNX",
		Handler:    hsetnxCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HDEL",
		Handler:    hdelCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HEXISTS",
		Handler:    hexistsCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HINCRBY",
		Handler:    hincrbyCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HINCRBYFLOAT",
		Handler:    hincrbyfloatCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HKEYS",
		Handler:    hkeysCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HVALS",
		Handler:    hvalsCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HGETALL",
		Handler:    hgetallCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HLEN",
		Handler:    hlenCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HSTRLEN",
		Handler:    hstrlenCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HSCAN",
		Handler:    hscanCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})

	disp.Register(&command.Command{
		Name:       "HRANDFIELD",
		Handler:    hrandfieldCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatHash},
	})
}

// HSET key field value [field value ...]
func hsetCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	// Get or create hash object
	obj, ok := ctx.DB.Get(key)
	var h *hash.Hash
	if !ok {
		obj = database.NewHashObject()
		ctx.DB.Set(key, obj)
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	} else {
		if obj.Type != database.ObjTypeHash {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	}

	// Set field-value pairs
	if len(args) == 3 {
		// Single field-value pair
		field := args[1]
		value := args[2]
		added := h.Set(field, value)
		return command.NewIntegerReply(int64(added)), nil
	}

	// Multiple field-value pairs
	// args = [key, field1, value1, field2, value2, ...]
	// After key, we need field-value pairs (even number of remaining args)
	if (len(args)-1)%2 != 0 {
		return nil, errors.New("wrong number of arguments for multiple field-value pairs")
	}

	added := 0
	for i := 1; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		added += h.Set(field, value)
	}

	return command.NewIntegerReply(int64(added)), nil
}

// HGET key field
func hgetCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	val, exists := h.Get(field)
	if !exists {
		return command.NewNilReply(), nil
	}

	return command.NewBulkStringReply(val), nil
}

// HMSET key field value [field value ...]
func hmsetCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	// Get or create hash object
	obj, ok := ctx.DB.Get(key)
	var h *hash.Hash
	if !ok {
		obj = database.NewHashObject()
		ctx.DB.Set(key, obj)
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	} else {
		if obj.Type != database.ObjTypeHash {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	}

	// Check for even number of field-value pairs
	if (len(args)-2)%2 != 0 {
		return nil, errors.New("wrong number of arguments for multiple field-value pairs")
	}

	pairs := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		pairs[field] = value
	}

	h.MSet(pairs)
	return command.NewStatusReply("OK"), nil
}

// HMGET key field [field ...]
func hmgetCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	fields := args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Return all nil values
		result := make([]interface{}, len(fields))
		for i := range result {
			result[i] = nil
		}
		return command.NewArrayReplyFromAny(result), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	result := h.MGet(fields)
	return command.NewArrayReplyFromAny(result), nil
}

// HSETNX key field value
func hsetnxCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]
	value := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	var h *hash.Hash
	if !ok {
		obj = database.NewHashObject()
		ctx.DB.Set(key, obj)
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	} else {
		if obj.Type != database.ObjTypeHash {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	}

	// Check if field exists
	if h.Exists(field) {
		return command.NewIntegerReply(0), nil
	}

	h.Set(field, value)
	return command.NewIntegerReply(1), nil
}

// HDEL key field [field ...]
func hdelCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	fields := args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	deleted := h.Del(fields...)

	// Delete the key if hash is empty
	if h.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(deleted)), nil
}

// HEXISTS key field
func hexistsCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	if h.Exists(field) {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// HINCRBY key field increment
func hincrbyCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]
	delta, err := strconv.ParseInt(ctx.Args[2], 10, 64)
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	var h *hash.Hash
	if !ok {
		obj = database.NewHashObject()
		ctx.DB.Set(key, obj)
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	} else {
		if obj.Type != database.ObjTypeHash {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	}

	newVal, err := h.IncrBy(field, delta)
	if err != nil {
		return nil, err
	}

	return command.NewIntegerReply(newVal), nil
}

// HINCRBYFLOAT key field increment
func hincrbyfloatCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]
	delta, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		return nil, errors.New("value is not a valid float")
	}

	obj, ok := ctx.DB.Get(key)
	var h *hash.Hash
	if !ok {
		obj = database.NewHashObject()
		ctx.DB.Set(key, obj)
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	} else {
		if obj.Type != database.ObjTypeHash {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		h, ok = obj.Ptr.(*hash.Hash)
		if !ok {
			return nil, errors.New("internal error: not a hash object")
		}
	}

	newVal, err := h.IncrByFloat(field, delta)
	if err != nil {
		return nil, err
	}

	return command.NewBulkStringReply(strconv.FormatFloat(newVal, 'f', -1, 64)), nil
}

// HKEYS key
func hkeysCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	keys := h.Keys()
	return command.NewStringArrayReply(keys), nil
}

// HVALS key
func hvalsCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	vals := h.Vals()
	return command.NewStringArrayReply(vals), nil
}

// HGETALL key
func hgetallCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	all := h.GetAll()
	return command.NewStringArrayReply(all), nil
}

// HLEN key
func hlenCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	return command.NewIntegerReply(int64(h.Len())), nil
}

// HSTRLEN key field
func hstrlenCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	field := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	return command.NewIntegerReply(int64(h.StrLen(field))), nil
}

// HSCAN key cursor [MATCH pattern] [COUNT count]
func hscanCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	cursor, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("invalid cursor")
	}

	// Default values
	count := 10
	pattern := "*"

	// Parse options
	i := 2
	for i < len(args) {
		switch args[i] {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			pattern = args[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			count, err = strconv.Atoi(args[i+1])
			if err != nil || count < 1 {
				return nil, errors.New("invalid count")
			}
			i += 2
		default:
			return nil, errors.New("syntax error")
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Empty hash
		result := []string{"0"}
		return command.NewStringArrayReply(result), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	newCursor, fields := h.Scan(cursor, count, pattern)

	// Build result: [cursor, field1, value1, field2, value2, ...]
	result := []string{strconv.Itoa(newCursor)}
	for _, field := range fields {
		val, _ := h.Get(field)
		result = append(result, field, val)
	}

	return command.NewStringArrayReply(result), nil
}

// HRANDFIELD key [count [WITHVALUES]]
func hrandfieldCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	withValues := false

	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("invalid count")
		}
		count = c

		if len(args) >= 3 && args[2] == "WITHVALUES" {
			withValues = true
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		if count < 0 {
			return command.NewStringArrayReply([]string{}), nil
		}
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeHash {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return nil, errors.New("internal error: not a hash object")
	}

	// Get all fields and return random ones
	keys := h.Keys()
	if len(keys) == 0 {
		if count < 0 {
			return command.NewStringArrayReply([]string{}), nil
		}
		return command.NewNilReply(), nil
	}

	// For simplicity, return first N fields (proper implementation would use random sampling)
	// Handle negative count (return with values)
	if count < 0 {
		count = -count
		withValues = true
	}

	if count > len(keys) {
		count = len(keys)
	}

	result := make([]string, 0, count*2)
	for i := 0; i < count; i++ {
		field := keys[i]
		if withValues {
			val, _ := h.Get(field)
			result = append(result, field, val)
		} else {
			result = append(result, field)
		}
	}

	if withValues {
		return command.NewStringArrayReply(result), nil
	}
	return command.NewStringArrayReply(result), nil
}
