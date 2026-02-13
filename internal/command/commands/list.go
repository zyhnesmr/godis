// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"strconv"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/list"
)

// RegisterListCommands registers all list commands
func RegisterListCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "LPUSH",
		Handler:    lpushCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "RPUSH",
		Handler:    rpushCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LPOP",
		Handler:    lpopCmd,
		Arity:      2,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "RPOP",
		Handler:    rpopCmd,
		Arity:      2,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LLEN",
		Handler:    llenCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LINDEX",
		Handler:    lindexCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LSET",
		Handler:    lsetCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LRANGE",
		Handler:    lrangeCmd,
		Arity:      4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LTRIM",
		Handler:    ltrimCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LREM",
		Handler:    lremCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})

	disp.Register(&command.Command{
		Name:       "LINSERT",
		Handler:    linsertCmd,
		Arity:      5,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatList},
	})
}

// LPUSH key value [value ...]
func lpushCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	values := args[1:]

	obj, ok := ctx.DB.Get(key)
	var l *list.List
	if !ok {
		obj = database.NewListObject()
		ctx.DB.Set(key, obj)
		var ok bool
		l, ok = obj.Ptr.(*list.List)
		if !ok {
			return nil, errors.New("internal error: not a list object")
		}
	} else {
		if obj.Type != database.ObjTypeList {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		l, ok = obj.Ptr.(*list.List)
		if !ok {
			return nil, errors.New("internal error: not a list object")
		}
	}

	// Push all values to the left (head)
	for _, value := range values {
		l.PushLeft(value)
	}

	return command.NewIntegerReply(int64(l.Len())), nil
}

// RPUSH key value [value ...]
func rpushCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	values := args[1:]

	obj, ok := ctx.DB.Get(key)
	var l *list.List
	if !ok {
		obj = database.NewListObject()
		ctx.DB.Set(key, obj)
		var ok bool
		l, ok = obj.Ptr.(*list.List)
		if !ok {
			return nil, errors.New("internal error: not a list object")
		}
	} else {
		if obj.Type != database.ObjTypeList {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		l, ok = obj.Ptr.(*list.List)
		if !ok {
			return nil, errors.New("internal error: not a list object")
		}
	}

	// Push all values to the right (tail)
	for _, value := range values {
		l.PushRight(value)
	}

	return command.NewIntegerReply(int64(l.Len())), nil
}

// LPOP key
func lpopCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	value, ok := l.PopLeft()
	if !ok {
		return command.NewNilReply(), nil
	}

	// Delete the key if list is empty
	if l.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewBulkStringReply(value), nil
}

// RPOP key
func rpopCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	value, ok := l.PopRight()
	if !ok {
		return command.NewNilReply(), nil
	}

	// Delete the key if list is empty
	if l.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewBulkStringReply(value), nil
}

// LLEN key
func llenCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	return command.NewIntegerReply(int64(l.Len())), nil
}

// LINDEX key index
func lindexCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	index, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	value, ok := l.Index(index)
	if !ok {
		return command.NewNilReply(), nil
	}

	return command.NewBulkStringReply(value), nil
}

// LSET key index value
func lsetCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	index, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	value := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return nil, errors.New("no such key")
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	ok = l.Set(index, value)
	if !ok {
		return command.NewNilReply(), nil
	}

	return command.NewStatusReply("OK"), nil
}

// LRANGE key start stop
func lrangeCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	start, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	stop, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	values := l.Range(start, stop)
	return command.NewStringArrayReply(values), nil
}

// LTRIM key start stop
func ltrimCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	start, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	stop, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	l.Trim(start, stop)
	return command.NewStatusReply("OK"), nil
}

// LREM key count value
func lremCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	count, err := strconv.Atoi(ctx.Args[1])
	if err != nil || count < -1 {
		return nil, errors.New("value is not an integer or out of range")
	}

	value := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	removed := l.Remove(value, count)

	// Delete the key if list is empty
	if l.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(removed)), nil
}

// LINSERT key BEFORE/AFTER pivot value
func linsertCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 4 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	where := args[1]
	pivot := args[2]
	value := args[3]

	// Validate where parameter
	if where != "BEFORE" && where != "AFTER" {
		return nil, errors.New("syntax error")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeList {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return nil, errors.New("internal error: not a list object")
	}

	var inserted bool
	if where == "BEFORE" {
		inserted = l.InsertBefore(pivot, value)
	} else {
		inserted = l.InsertAfter(pivot, value)
	}

	if !inserted {
		return command.NewNilReply(), nil
	}

	return command.NewIntegerReply(int64(l.Len())), nil
}
