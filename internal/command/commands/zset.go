// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/zset"
)

// RegisterZSetCommands registers all sorted set commands
func RegisterZSetCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "ZADD",
		Handler:    zaddCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREM",
		Handler:    zremCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZSCORE",
		Handler:    zscoreCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZMSCORE",
		Handler:    zmscoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZINCRBY",
		Handler:    zincrbyCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZCARD",
		Handler:    zcardCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZCOUNT",
		Handler:    zcountCmd,
		Arity:      4,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZRANGE",
		Handler:    zrangeCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREVRANGE",
		Handler:    zrevrangeCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZRANGEBYSCORE",
		Handler:    zrangebyscoreCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREVRANGEBYSCORE",
		Handler:    zrevrangebyscoreCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZRANK",
		Handler:    zrankCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREVRANK",
		Handler:    zrevrankCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZPOPMAX",
		Handler:    zpopmaxCmd,
		Arity:      -2,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZPOPMIN",
		Handler:    zpopminCmd,
		Arity:      -2,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREMRANGEBYRANK",
		Handler:    zremrangebyrankCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZREMRANGEBYSCORE",
		Handler:    zremrangebyscoreCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZUNION",
		Handler:    zunionCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   0,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZINTER",
		Handler:    zinterCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   0,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZUNIONSTORE",
		Handler:    zunionstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZINTERSTORE",
		Handler:    zinterstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZDIFF",
		Handler:    zdiffCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly, command.FlagSortForScript},
		FirstKey:   2,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZDIFFSTORE",
		Handler:    zdiffstoreCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagSortForScript},
		FirstKey:   1,
		LastKey:    -1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZSCAN",
		Handler:    zscanCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})

	disp.Register(&command.Command{
		Name:       "ZRANDMEMBER",
		Handler:    zrandmemberCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagRandom},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatZSet},
	})
}

// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
func zaddCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	// Parse options
	nx := false
	xx := false
	ch := false
	incr := false
	idx := 1

OptionsLoop:
	for idx < len(args) {
		switch strings.ToUpper(args[idx]) {
		case "NX":
			nx = true
			idx++
		case "XX":
			xx = true
			idx++
		case "CH":
			ch = true
			idx++
		case "INCR":
			incr = true
			idx++
		default:
			break OptionsLoop
		}
	}

	if incr && len(args)-idx != 2 {
		return nil, errors.New("INCR option requires exactly one score-member pair")
	}

	// Get or create zset object
	obj, ok := ctx.DB.Get(key)
	var zs *zset.ZSet
	if !ok {
		if xx {
			// XX means only update existing elements
			return command.NewIntegerReply(0), nil
		}
		obj = database.NewZSetObject()
		ctx.DB.Set(key, obj)
		var ok bool
		zs, ok = obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
	} else {
		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		zs, ok = obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
	}

	if incr {
		// ZADD INCR score member
		score, err := strconv.ParseFloat(args[idx], 64)
		if err != nil {
			return nil, errors.New("value is not a valid float")
		}
		member := args[idx+1]

		if nx {
			if _, exists := zs.Score(member); exists {
				return command.NewNilReply(), nil
			}
		}
		if xx {
			if _, exists := zs.Score(member); !exists {
				return command.NewNilReply(), nil
			}
		}

		newScore := zs.IncrBy(member, score)
		return command.NewBulkStringReply(strconv.FormatFloat(newScore, 'f', -1, 64)), nil
	}

	// Parse score-member pairs
	members := []zset.ZMember{}
	added := 0
	changed := 0

	for i := idx; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return nil, errors.New("syntax error")
		}

		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return nil, errors.New("value is not a valid float")
		}
		member := args[i+1]

		if nx {
			if _, exists := zs.Score(member); exists {
				continue
			}
		}
		if xx {
			if _, exists := zs.Score(member); !exists {
				continue
			}
		}

		oldScore, exists := zs.Score(member)
		if !exists {
			added++
		} else if oldScore != score {
			changed++
		}

		members = append(members, zset.ZMember{Member: member, Score: score})
	}

	zs.AddMultiple(members)

	if ch {
		return command.NewIntegerReply(int64(added + changed)), nil
	}
	return command.NewIntegerReply(int64(added)), nil
}

// ZREM key member [member ...]
func zremCmd(ctx *command.Context) (*command.Reply, error) {
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

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	removed := zs.RemoveMultiple(members)

	// Delete the key if zset is empty
	if zs.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(removed)), nil
}

// ZSCORE key member
func zscoreCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	score, exists := zs.Score(member)
	if !exists {
		return command.NewNilReply(), nil
	}

	return command.NewBulkStringReply(strconv.FormatFloat(score, 'f', -1, 64)), nil
}

// ZMSCORE key member [member ...]
func zmscoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	members := args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Return all nil
		result := make([]interface{}, len(members))
		for i := range result {
			result[i] = nil
		}
		return command.NewArrayReplyFromAny(result), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	result := zs.ScoreMultiple(members)
	return command.NewArrayReplyFromAny(result), nil
}

// ZINCRBY key increment member
func zincrbyCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	increment, err := strconv.ParseFloat(ctx.Args[1], 64)
	if err != nil {
		return nil, errors.New("value is not a valid float")
	}
	member := ctx.Args[2]

	// Get or create zset object
	obj, ok := ctx.DB.Get(key)
	var zs *zset.ZSet
	if !ok {
		obj = database.NewZSetObject()
		ctx.DB.Set(key, obj)
		var ok bool
		zs, ok = obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
	} else {
		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}
		var ok bool
		zs, ok = obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
	}

	newScore := zs.IncrBy(member, increment)
	return command.NewBulkStringReply(strconv.FormatFloat(newScore, 'f', -1, 64)), nil
}

// ZCARD key
func zcardCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	return command.NewIntegerReply(int64(zs.Len())), nil
}

// ZCOUNT key min max
func zcountCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	minStr := ctx.Args[1]
	maxStr := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	min, max := parseScoreRange(minStr, maxStr)
	count := zs.Count(min, max)

	return command.NewIntegerReply(int64(count)), nil
}

// ZRANGE key start stop [WITHSCORES]
func zrangeCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}

	withScores := false
	if len(args) >= 4 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	members := zs.Range(start, stop)
	return formatZMembers(members, withScores), nil
}

// ZREVRANGE key start stop [WITHSCORES]
func zrevrangeCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}

	withScores := false
	if len(args) >= 4 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	members := zs.RevRange(start, stop)
	return formatZMembers(members, withScores), nil
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func zrangebyscoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	minStr := args[1]
	maxStr := args[2]

	withScores := false
	offset := 0
	count := -1

	// Parse options
	i := 3
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, errors.New("syntax error")
			}
			off, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("value is not an integer")
			}
			cnt, err := strconv.Atoi(args[i+2])
			if err != nil {
				return nil, errors.New("value is not an integer")
			}
			offset = off
			count = cnt
			i += 3
		default:
			return nil, errors.New("syntax error")
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	min, max := parseScoreRange(minStr, maxStr)
	members := zs.RangeByScore(min, max)

	// Apply LIMIT
	if offset > 0 || count >= 0 {
		if offset < 0 {
			offset = 0
		}
		if offset >= len(members) {
			return command.NewStringArrayReply([]string{}), nil
		}
		end := len(members)
		if count >= 0 && offset+count < end {
			end = offset + count
		}
		members = members[offset:end]
	}

	return formatZMembers(members, withScores), nil
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
func zrevrangebyscoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	maxStr := args[1]
	minStr := args[2]

	withScores := false
	offset := 0
	count := -1

	// Parse options
	i := 3
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, errors.New("syntax error")
			}
			off, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("value is not an integer")
			}
			cnt, err := strconv.Atoi(args[i+2])
			if err != nil {
				return nil, errors.New("value is not an integer")
			}
			offset = off
			count = cnt
			i += 3
		default:
			return nil, errors.New("syntax error")
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	min, max := parseScoreRange(minStr, maxStr)
	members := zs.RangeByScore(min, max)

	// Reverse the result
	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}

	// Apply LIMIT
	if offset > 0 || count >= 0 {
		if offset < 0 {
			offset = 0
		}
		if offset >= len(members) {
			return command.NewStringArrayReply([]string{}), nil
		}
		end := len(members)
		if count >= 0 && offset+count < end {
			end = offset + count
		}
		members = members[offset:end]
	}

	return formatZMembers(members, withScores), nil
}

// ZRANK key member
func zrankCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	rank := zs.Rank(member)
	if rank == -1 {
		return command.NewNilReply(), nil
	}

	return command.NewIntegerReply(rank), nil
}

// ZREVRANK key member
func zrevrankCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member := ctx.Args[1]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	rank := zs.RevRank(member)
	if rank == -1 {
		return command.NewNilReply(), nil
	}

	return command.NewIntegerReply(rank), nil
}

// ZPOPMAX key [count]
func zpopmaxCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("value is not an integer")
		}
		if c < 0 {
			return nil, errors.New("value is out of range")
		}
		count = c
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	if count == 1 {
		member, exists := zs.PopMax()
		if !exists {
			return command.NewNilReply(), nil
		}
		if zs.Len() == 0 {
			ctx.DB.Delete(key)
		}
		result := []string{
			member.Member,
			strconv.FormatFloat(member.Score, 'f', -1, 64),
		}
		return command.NewStringArrayReply(result), nil
	}

	members := zs.PopMaxMultiple(count)

	if zs.Len() == 0 {
		ctx.DB.Delete(key)
	}

	if len(members) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	result := []string{}
	for _, m := range members {
		result = append(result, m.Member, strconv.FormatFloat(m.Score, 'f', -1, 64))
	}

	return command.NewStringArrayReply(result), nil
}

// ZPOPMIN key [count]
func zpopminCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("value is not an integer")
		}
		if c < 0 {
			return nil, errors.New("value is out of range")
		}
		count = c
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	if count == 1 {
		member, exists := zs.PopMin()
		if !exists {
			return command.NewNilReply(), nil
		}
		if zs.Len() == 0 {
			ctx.DB.Delete(key)
		}
		result := []string{
			member.Member,
			strconv.FormatFloat(member.Score, 'f', -1, 64),
		}
		return command.NewStringArrayReply(result), nil
	}

	members := zs.PopMinMultiple(count)

	if zs.Len() == 0 {
		ctx.DB.Delete(key)
	}

	if len(members) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	result := []string{}
	for _, m := range members {
		result = append(result, m.Member, strconv.FormatFloat(m.Score, 'f', -1, 64))
	}

	return command.NewStringArrayReply(result), nil
}

// ZREMRANGEBYRANK key start stop
func zremrangebyrankCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	start, err := strconv.Atoi(ctx.Args[1])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}
	stop, err := strconv.Atoi(ctx.Args[2])
	if err != nil {
		return nil, errors.New("value is not an integer")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	removed := zs.RemoveRangeByRank(start, stop)

	if zs.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(removed)), nil
}

// ZREMRANGEBYSCORE key min max
func zremrangebyscoreCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	minStr := ctx.Args[1]
	maxStr := ctx.Args[2]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	min, max := parseScoreRange(minStr, maxStr)
	removed := zs.RemoveRangeByScore(min, max)

	if zs.Len() == 0 {
		ctx.DB.Delete(key)
	}

	return command.NewIntegerReply(int64(removed)), nil
}

// ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func zunionCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 1+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[1 : 1+numKeys]
	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "sum"
	idx := 1 + numKeys

	// Parse options
	for idx < len(args) {
		switch strings.ToUpper(args[idx]) {
		case "WEIGHTS":
			if idx+numKeys >= len(args) {
				return nil, errors.New("syntax error")
			}
			for i := 0; i < numKeys; i++ {
				w, err := strconv.ParseFloat(args[idx+1+i], 64)
				if err != nil {
					return nil, errors.New("value is not a valid float")
				}
				weights[i] = w
			}
			idx += 1 + numKeys
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			aggregate = strings.ToLower(args[idx+1])
			idx += 2
		default:
			return nil, errors.New("syntax error")
		}
	}

	// Collect zsets
	sets := []*zset.ZSet{}
	for i, key := range keys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		zs, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}

		// Apply weights if needed
		if weights[i] != 1.0 {
			// Create a copy with weighted scores
			members := zs.GetAll()
			weightedMembers := make([]zset.ZMember, len(members))
			for j, m := range members {
				weightedMembers[j] = zset.ZMember{
					Member: m.Member,
					Score:  m.Score * weights[i],
				}
			}
			newZs := zset.NewZSet()
			newZs.AddMultiple(weightedMembers)
			sets = append(sets, newZs)
		} else {
			sets = append(sets, zs)
		}
	}

	if len(sets) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	// Compute union
	result := sets[0].Union(sets[1:], aggregate)
	return formatZMembers(result, true), nil
}

// ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func zinterCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 1+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[1 : 1+numKeys]
	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "sum"
	idx := 1 + numKeys

	// Parse options
	for idx < len(args) {
		switch strings.ToUpper(args[idx]) {
		case "WEIGHTS":
			if idx+numKeys >= len(args) {
				return nil, errors.New("syntax error")
			}
			for i := 0; i < numKeys; i++ {
				w, err := strconv.ParseFloat(args[idx+1+i], 64)
				if err != nil {
					return nil, errors.New("value is not a valid float")
				}
				weights[i] = w
			}
			idx += 1 + numKeys
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			aggregate = strings.ToLower(args[idx+1])
			idx += 2
		default:
			return nil, errors.New("syntax error")
		}
	}

	// Collect zsets
	sets := []*zset.ZSet{}
	for i, key := range keys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			// If any key doesn't exist, intersection is empty
			return command.NewStringArrayReply([]string{}), nil
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		zs, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}

		// Apply weights if needed
		if weights[i] != 1.0 {
			// Create a copy with weighted scores
			members := zs.GetAll()
			weightedMembers := make([]zset.ZMember, len(members))
			for j, m := range members {
				weightedMembers[j] = zset.ZMember{
					Member: m.Member,
					Score:  m.Score * weights[i],
				}
			}
			newZs := zset.NewZSet()
			newZs.AddMultiple(weightedMembers)
			sets = append(sets, newZs)
		} else {
			sets = append(sets, zs)
		}
	}

	if len(sets) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	// Compute intersection
	result := sets[0].Intersect(sets[1:], aggregate)
	return formatZMembers(result, true), nil
}

// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func zunionstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 2+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[2 : 2+numKeys]
	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "sum"
	idx := 2 + numKeys

	// Parse options
	for idx < len(args) {
		switch strings.ToUpper(args[idx]) {
		case "WEIGHTS":
			if idx+numKeys >= len(args) {
				return nil, errors.New("syntax error")
			}
			for i := 0; i < numKeys; i++ {
				w, err := strconv.ParseFloat(args[idx+1+i], 64)
				if err != nil {
					return nil, errors.New("value is not a valid float")
				}
				weights[i] = w
			}
			idx += 1 + numKeys
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			aggregate = strings.ToLower(args[idx+1])
			idx += 2
		default:
			return nil, errors.New("syntax error")
		}
	}

	// Collect zsets
	sets := []*zset.ZSet{}
	for i, key := range keys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		zs, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}

		// Apply weights if needed
		if weights[i] != 1.0 {
			// Create a copy with weighted scores
			members := zs.GetAll()
			weightedMembers := make([]zset.ZMember, len(members))
			for j, m := range members {
				weightedMembers[j] = zset.ZMember{
					Member: m.Member,
					Score:  m.Score * weights[i],
				}
			}
			newZs := zset.NewZSet()
			newZs.AddMultiple(weightedMembers)
			sets = append(sets, newZs)
		} else {
			sets = append(sets, zs)
		}
	}

	// Compute union
	var result []zset.ZMember
	if len(sets) == 0 {
		result = []zset.ZMember{}
	} else {
		result = sets[0].Union(sets[1:], aggregate)
	}

	// Create new zset with result
	newZs := zset.NewZSet()
	newZs.AddMultiple(result)

	// Store result
	obj := database.NewZSetObject()
	obj.Ptr = newZs
	ctx.DB.Set(dstKey, obj)

	return command.NewIntegerReply(int64(len(result))), nil
}

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func zinterstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 2+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[2 : 2+numKeys]
	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "sum"
	idx := 2 + numKeys

	// Parse options
	for idx < len(args) {
		switch strings.ToUpper(args[idx]) {
		case "WEIGHTS":
			if idx+numKeys >= len(args) {
				return nil, errors.New("syntax error")
			}
			for i := 0; i < numKeys; i++ {
				w, err := strconv.ParseFloat(args[idx+1+i], 64)
				if err != nil {
					return nil, errors.New("value is not a valid float")
				}
				weights[i] = w
			}
			idx += 1 + numKeys
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			aggregate = strings.ToLower(args[idx+1])
			idx += 2
		default:
			return nil, errors.New("syntax error")
		}
	}

	// Collect zsets
	sets := []*zset.ZSet{}
	for i, key := range keys {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			// If any key doesn't exist, intersection is empty
			ctx.DB.Delete(dstKey)
			return command.NewIntegerReply(0), nil
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		zs, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}

		// Apply weights if needed
		if weights[i] != 1.0 {
			// Create a copy with weighted scores
			members := zs.GetAll()
			weightedMembers := make([]zset.ZMember, len(members))
			for j, m := range members {
				weightedMembers[j] = zset.ZMember{
					Member: m.Member,
					Score:  m.Score * weights[i],
				}
			}
			newZs := zset.NewZSet()
			newZs.AddMultiple(weightedMembers)
			sets = append(sets, newZs)
		} else {
			sets = append(sets, zs)
		}
	}

	if len(sets) == 0 {
		ctx.DB.Delete(dstKey)
		return command.NewIntegerReply(0), nil
	}

	// Compute intersection
	result := sets[0].Intersect(sets[1:], aggregate)

	// Create new zset with result
	newZs := zset.NewZSet()
	newZs.AddMultiple(result)

	// Store result
	obj := database.NewZSetObject()
	obj.Ptr = newZs
	ctx.DB.Set(dstKey, obj)

	return command.NewIntegerReply(int64(len(result))), nil
}

// ZDIFF numkeys key [key ...] [WITHSCORES]
func zdiffCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 1+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[1 : 1+numKeys]

	withScores := false
	if len(args) > 1+numKeys && strings.ToUpper(args[1+numKeys]) == "WITHSCORES" {
		withScores = true
	}

	// Get first set (the one we're subtracting from)
	obj, ok := ctx.DB.Get(keys[0])
	if !ok {
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	// Collect other sets
	others := []*zset.ZSet{}
	for _, key := range keys[1:] {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		other, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
		others = append(others, other)
	}

	result := zs.Diff(others)
	return formatZMembers(result, withScores), nil
}

// ZDIFFSTORE destination numkeys key [key ...]
func zdiffstoreCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	dstKey := args[0]
	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 1 {
		return nil, errors.New("value is not a valid integer or out of range")
	}

	if len(args) < 2+numKeys {
		return nil, errors.New("wrong number of arguments")
	}

	keys := args[2 : 2+numKeys]

	// Get first set (the one we're subtracting from)
	obj, ok := ctx.DB.Get(keys[0])
	if !ok {
		ctx.DB.Delete(dstKey)
		return command.NewIntegerReply(0), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	// Collect other sets
	others := []*zset.ZSet{}
	for _, key := range keys[1:] {
		obj, ok := ctx.DB.Get(key)
		if !ok {
			continue
		}

		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("wrong type operation against a key holding another kind of value")
		}

		other, ok := obj.Ptr.(*zset.ZSet)
		if !ok {
			return nil, errors.New("internal error: not a zset object")
		}
		others = append(others, other)
	}

	result := zs.Diff(others)

	// Create new zset with result
	newZs := zset.NewZSet()
	newZs.AddMultiple(result)

	// Store result
	obj2 := database.NewZSetObject()
	obj2.Ptr = newZs
	ctx.DB.Set(dstKey, obj2)

	return command.NewIntegerReply(int64(len(result))), nil
}

// ZSCAN key cursor [MATCH pattern] [COUNT count]
func zscanCmd(ctx *command.Context) (*command.Reply, error) {
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
		// Empty zset
		result := []string{"0"}
		return command.NewStringArrayReply(result), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	newCursor, members := zs.Scan(cursor, count)

	// Build result: [cursor, member1, score1, member2, score2, ...]
	result := []string{strconv.Itoa(newCursor)}
	for _, m := range members {
		result = append(result, m.Member, strconv.FormatFloat(m.Score, 'f', -1, 64))
	}

	return command.NewStringArrayReply(result), nil
}

// ZRANDMEMBER key [count [WITHSCORES]]
func zrandmemberCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	count := 1
	withScores := false

	if len(args) >= 2 {
		c, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("value is not an integer")
		}
		count = c

		if len(args) >= 3 && strings.ToUpper(args[2]) == "WITHSCORES" {
			withScores = true
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		if count == 1 {
			return command.NewNilReply(), nil
		}
		return command.NewStringArrayReply([]string{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("wrong type operation against a key holding another kind of value")
	}

	zs, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return nil, errors.New("internal error: not a zset object")
	}

	if count == 1 && !withScores {
		// Return single member without score
		members := zs.GetAll()
		if len(members) == 0 {
			return command.NewNilReply(), nil
		}
		// Return first member (simple implementation, proper one would be random)
		return command.NewBulkStringReply(members[0].Member), nil
	}

	members := zs.GetAll()
	if len(members) == 0 {
		return command.NewStringArrayReply([]string{}), nil
	}

	// Simple implementation: return first count members
	// Proper implementation would use random selection
	if count < 0 {
		count = -count
		// Return with duplicates
		result := []string{}
		for i := 0; i < count && len(members) > 0; i++ {
			idx := i % len(members)
			result = append(result, members[idx].Member)
			if withScores {
				result = append(result, strconv.FormatFloat(members[idx].Score, 'f', -1, 64))
			}
		}
		return command.NewStringArrayReply(result), nil
	}

	if count > len(members) {
		count = len(members)
	}

	result := []string{}
	for i := 0; i < count; i++ {
		result = append(result, members[i].Member)
		if withScores {
			result = append(result, strconv.FormatFloat(members[i].Score, 'f', -1, 64))
		}
	}

	return command.NewStringArrayReply(result), nil
}

// Helper functions

func parseScoreRange(minStr, maxStr string) (min float64, max float64) {
	min = parseScoreBound(minStr)
	max = parseScoreBound(maxStr)
	return
}

func parseScoreBound(s string) float64 {
	switch s {
	case "-inf", "-Infinity":
		return math.Inf(-1)
	case "+inf", "Infinity", "inf":
		return math.Inf(1)
	default:
		exclusive := false
		if len(s) > 0 && s[0] == '(' {
			exclusive = true
			s = s[1:]
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		if exclusive {
			// For simplicity, we don't adjust for exclusivity
			// A proper implementation would use nextafter
		}
		return f
	}
}

func formatZMembers(members []zset.ZMember, withScores bool) *command.Reply {
	if !withScores {
		result := make([]string, len(members))
		for i, m := range members {
			result[i] = m.Member
		}
		return command.NewStringArrayReply(result)
	}

	result := make([]string, 0, len(members)*2)
	for _, m := range members {
		result = append(result, m.Member, strconv.FormatFloat(m.Score, 'f', -1, 64))
	}
	return command.NewStringArrayReply(result)
}
