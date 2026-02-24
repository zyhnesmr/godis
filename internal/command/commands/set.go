// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"strconv"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/set"
)

// RegisterSetCommands registers all set commands
func RegisterSetCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "SADD",
		Handler:    saddCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SREM",
		Handler:    sremCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SPOP",
		Handler:    spopCmd,
		Arity:      -2,
		Flags:      []string{command.FlagWrite, command.FlagRandom, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SRANDMEMBER",
		Handler:    srandmemberCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagRandom, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SISMEMBER",
		Handler:    sismemberCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SMISMEMBER",
		Handler:    smismemberCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SMEMBERS",
		Handler:    smembersCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SCARD",
		Handler:    scardCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SMOVE",
		Handler:    smoveCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    2,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SINTER",
		Handler:    sinterCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SINTERSTORE",
		Handler:    sinterstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SUNION",
		Handler:    sunionCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SUNIONSTORE",
		Handler:    sunionstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SDIFF",
		Handler:    sdiffCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SDIFFSTORE",
		Handler:    sdiffstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatSet},
	})

	disp.Register(&command.Command{
		Name:       "SSCAN",
		Handler:    sscanCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatSet},
	})
}

// SADD key member [member ...]
func saddCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	members := args[1:]

	// Get or create set object
	obj, ok := ctx.DB.Get(key)
	var s *set.Set
	if !ok {
		obj = database.NewSetObject()
		ctx.DB.Set(key, obj)
		var ok bool
		s, ok = obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
	} else {
		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		s, ok = obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
	}

	added := s.AddMultiple(members)
	return command.NewIntegerReply(int64(added)), nil
}

// SREM key member [member ...]
func sremCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	members := args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	removed := s.RemoveMultiple(members)

	// Delete the key if set is empty
	if s.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(removed)), nil
}

// SPOP key [count]
func spopCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("value is not an integer or out of range")
		}
		if c < 0 {
			return nil, errors.New("value is out of range")
		}
		count = c
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		if count == 1 {
			return command.NewNilReply(), nil
		}
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	if count == 1 {
		member, exists := s.Pop()
		if !exists {
			return command.NewNilReply(), nil
		}
		// Delete the key if set is empty
		if s.Len() == 0 {
			ctx.DB.Delete(key)
		}
		return command.NewBulkStringReply(member), nil
	}

	members := s.PopMultiple(count)

	// Delete the key if set is empty
	if s.Len() == 0 {
		ctx.DB.Delete(key)
	}

	if len(members) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}
	return command.NewStringArrayReply(members), nil
}

// SRANDMEMBER key [count]
func srandmemberCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("value is not an integer or out of range")
		}
		count = c
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		if count == 1 {
			return command.NewNilReply(), nil
		}
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	if count == 1 {
		member, exists := s.RandomMember()
		if !exists {
			return command.NewNilReply(), nil
		}
		return command.NewBulkStringReply(member), nil
	}

	if count < 0 {
		// Return with duplicates
		count = -count
		members := s.RandomMembers(count)
		if len(members) == 0 {
			return command.NewStringArrayReply([]string{}), nil
		}
		return command.NewStringArrayReply(members), nil
	}

	// Return distinct members
	members := s.RandomMembersDistinct(count)
	if len(members) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}
	return command.NewStringArrayReply(members), nil
}

// SISMEMBER key member
func sismemberCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	if s.Contains(member) {
		return command.NewIntegerReply(1), nil
	}
	return command.NewIntegerReply(0), nil
}

// SMISMEMBER key member [member ...]
func smismemberCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	members := args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Return all zeros
		result := make([]interface{}, len(members))
		for i := range result {
			result[i] = int64(0)
		}
		return command.NewArrayReplyFromAny(result), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	result := s.ContainsMultiple(members)
	resultInt := make([]interface{}, len(result))
	for i, v := range result {
		resultInt[i] = int64(v)
	}
	return command.NewArrayReplyFromAny(resultInt), nil
}

// SMEMBERS key
func smembersCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	members := s.Members()
	return command.NewStringArrayReply(members), nil
}

// SCARD key
func scardCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	return command.NewIntegerReply(int64(s.Len())), nil
}

// SMOVE source destination member
func smoveCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	srcKey := ctx.Args[0]
	dstKey := ctx.Args[1]
	member := ctx.Args[2]

	// Get source set
	srcObj, srcOk := ctx.DB.Get(srcKey)
	if !srcOk {
		return command.NewIntegerReply(0), nil
	}

	if srcObj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	srcSet, ok := srcObj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	// Get or create destination set
	dstObj, dstOk := ctx.DB.Get(dstKey)
	var dstSet *set.Set
	if !dstOk {
		dstObj = database.NewSetObject()
		ctx.DB.Set(dstKey, dstObj)
		var ok bool
		dstSet, ok = dstObj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
	} else {
		if dstObj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		dstSet, ok = dstObj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
	}

	// Move member
	if srcSet.MoveTo(member, dstSet) {
		// Delete source key if empty
		if srcSet.Len() == 0 {
			ctx.DB.Delete(srcKey)
		}
		return command.NewIntegerReply(1), nil
	}

	return command.NewIntegerReply(0), nil
}

// SINTER key [key ...]
func sinterCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	// Collect all sets
	sets := make([]*set.Set, 0, len(args))
	for _, key := range args {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			// If any set doesn't exist, intersection is empty
			return command.NewStringArrayReply([]string{}), nil
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	// First set intersects with the rest
	result := sets[0].Intersect(sets[1:])
	return command.NewStringArrayReply(result), nil
}

// SINTERSTORE destination key [key ...]
func sinterstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	srcKeys := args[1:]

	// Collect all sets
	sets := make([]*set.Set, 0, len(srcKeys))
	for _, key := range srcKeys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			// If any set doesn't exist, intersection is empty
			// Create empty destination set
			ctx.DB.Delete(dstKey)
			return command.NewIntegerReply(0), nil
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		ctx.DB.Delete(dstKey)
		return command.NewIntegerReply(0), nil
	}

	// Get intersection
	result := sets[0].Intersect(sets[1:])

	// Create new set with result
	newSet := set.NewSetFromSlice(result)
	obj := database.NewSetObject()
	obj.Ptr = newSet
	ctx.DB.Set(dstKey, obj)

	return command.NewIntegerReply(int64(len(result))), nil
}

// SUNION key [key ...]
func sunionCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	// Collect all sets
	sets := make([]*set.Set, 0, len(args))
	for _, key := range args {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	// First set unions with the rest
	result := sets[0].Union(sets[1:])
	return command.NewStringArrayReply(result), nil
}

// SUNIONSTORE destination key [key ...]
func sunionstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	srcKeys := args[1:]

	// Collect all sets
	sets := make([]*set.Set, 0, len(srcKeys))
	for _, key := range srcKeys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		sets = append(sets, s)
	}

	// Get union
	var result []string
	if len(sets) == 0 {
		result = []string{}
	} else {
		result = sets[0].Union(sets[1:])
	}

	// Create new set with result
	newSet := set.NewSetFromSlice(result)
	obj := database.NewSetObject()
	obj.Ptr = newSet
	ctx.DB.Set(dstKey, obj)

	return command.NewIntegerReply(int64(len(result))), nil
}

// SDIFF key [key ...]
func sdiffCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	// Get the first set
	obj, ok := ctx.DB.Get(args[0])
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	firstSet, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	// Collect other sets
	others := make([]*set.Set, 0, len(args)-1)
	for _, key := range args[1:] {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		others = append(others, s)
	}

	result := firstSet.Diff(others)
	return command.NewStringArrayReply(result), nil
}

// SDIFFSTORE destination key [key ...]
func sdiffstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	srcKeys := args[1:]

	// Get the first set
	obj, ok := ctx.DB.Get(srcKeys[0])
	if !ok {
		// First set doesn't exist, result is empty
		ctx.DB.Delete(dstKey)
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	firstSet, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	// Collect other sets
	others := make([]*set.Set, 0, len(srcKeys)-1)
	for _, key := range srcKeys[1:] {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		s, ok := obj.Ptr.(*set.Set)
		if !ok {
			return nil, errors.New("internal error: not a set object")
		}
		others = append(others, s)
	}

	result := firstSet.Diff(others)

	// Create new set with result
	newSet := set.NewSetFromSlice(result)
	obj2 := database.NewSetObject()
	obj2.Ptr = newSet
	ctx.DB.Set(dstKey, obj2)

	return command.NewIntegerReply(int64(len(result))), nil
}

// SSCAN key cursor [MATCH pattern] [COUNT count]
func sscanCmd(ctx *command.Context) (*command.Reply, error) {
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

	// Parse options (MATCH is ignored for now)
	i := 2
	for i < len(args) {
		switch args[i] {
		case "MATCH":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			// pattern = args[i+1] // Pattern not implemented yet
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
		// Empty set
		result := []string{"0"}
		return command.NewStringArrayReply(result), nil
	}

	if obj.Type != database.ObjTypeSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return nil, errors.New("internal error: not a set object")
	}

	newCursor, members := s.Scan(cursor, count)

	// Build result: [cursor, member1, member2, ...]
	result := []string{strconv.Itoa(newCursor)}
	result = append(result, members...)

	return command.NewStringArrayReply(result), nil
}
