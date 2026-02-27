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
	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/stream"
)

// RegisterStreamCommands registers all stream-related commands
func RegisterStreamCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "XADD",
		Handler:    xaddCmd,
		Arity:      -5,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XLEN",
		Handler:    xlenCmd,
		Arity:      2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XRANGE",
		Handler:    xrangeCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XREVRANGE",
		Handler:    xrevrangeCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XREAD",
		Handler:    xreadCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   2,
		LastKey:    -2,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XDEL",
		Handler:    xdelCmd,
		Arity:      -3,
		Flags:      []string{command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XTRIM",
		Handler:    xtrimCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XGROUP",
		Handler:    xgroupCmd,
		Arity:      -2,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XREADGROUP",
		Handler:    xreadgroupCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite},
		FirstKey:   1,
		LastKey:    -2,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XACK",
		Handler:    xackCmd,
		Arity:      -4,
		Flags:      []string{command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XCLAIM",
		Handler:    xclaimCmd,
		Arity:      -5,
		Flags:      []string{command.FlagWrite, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XPENDING",
		Handler:    xpendingCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
	disp.Register(&command.Command{
		Name:       "XINFO",
		Handler:    xinfoCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatStream},
	})
}

// XADD adds a new entry to a stream
func xaddCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 4 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	idStr := args[1]

	// Parse field-value pairs
	fields := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return nil, errors.New("wrong number of arguments for XADD")
		}
		fields[args[i]] = args[i+1]
	}

	// Get or create stream
	obj, exists := ctx.DB.Get(key)
	var strm *stream.Stream
	if !exists {
		strm = stream.NewStream()
		ctx.DB.Set(key, database.NewStreamObject())
		// Re-get to have the proper object
		obj, _ = ctx.DB.Get(key)
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm = strmVal.(*stream.Stream)

	// Parse ID
	var id stream.StreamID
	var err error
	if idStr == "*" || idStr == "0-0" {
		// Auto-generate ID
		id = strm.Add(fields)
	} else {
		id, err = stream.ParseStreamID(idStr)
		if err != nil {
			return nil, fmt.Errorf("Invalid stream ID specified: %w", err)
		}
		if err := strm.AddWithID(id, fields); err != nil {
			return nil, err
		}
	}

	return command.NewBulkStringReply(id.String()), nil
}

// XLEN returns the number of entries in a stream
func xlenCmd(ctx *command.Context) (*command.Reply, error) {
	key := ctx.Args[0]

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewIntegerReply(0), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	return command.NewIntegerReply(strm.Length()), nil
}

// XRANGE returns entries in a stream within a range
func xrangeCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	start := args[1]
	end := args[2]

	count := int64(0)
	if len(args) >= 5 && strings.ToUpper(args[3]) == "COUNT" {
		c, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return nil, errors.New("value is not an integer or out of range")
		}
		count = c
	}

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewArrayReply(nil), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	entries := strm.Range(start, end, count)
	return formatStreamEntries(entries), nil
}

// XREVRANGE returns entries in reverse order
func xrevrangeCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	end := args[1]
	start := args[2]

	count := int64(0)
	if len(args) >= 5 && strings.ToUpper(args[3]) == "COUNT" {
		c, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return nil, errors.New("value is not an integer or out of range")
		}
		count = c
	}

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewArrayReply(nil), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	entries := strm.RevRange(start, end, count)
	return formatStreamEntries(entries), nil
}

// XREAD reads entries from multiple streams
func xreadCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	count := int64(0)

	// Parse options
	idx := 0
	for idx < len(args) {
		arg := strings.ToUpper(args[idx])
		if arg == "COUNT" {
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			c, err := strconv.ParseInt(args[idx+1], 10, 64)
			if err != nil {
				return nil, errors.New("value is not an integer or out of range")
			}
			count = c
			idx += 2
		} else if arg == "BLOCK" {
			// Blocking not implemented yet, just skip
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			idx += 2
		} else if arg == "STREAMS" {
			idx++
			break
		} else {
			idx++
		}
	}

	// Find STREAMS keyword
	streamsIdx := -1
	for i, arg := range args {
		if strings.ToUpper(arg) == "STREAMS" {
			streamsIdx = i
			break
		}
	}

	if streamsIdx == -1 || streamsIdx == len(args)-1 {
		return nil, errors.New("syntax error")
	}

	streamsIdx++

	// Parse streams and IDs
	streamCount := (len(args) - streamsIdx) / 2
	if streamCount == 0 {
		return nil, errors.New("syntax error")
	}

	results := make([]*command.Reply, 0)

	for i := 0; i < streamCount; i++ {
		keyIdx := streamsIdx + i
		idIdx := streamsIdx + streamCount + i

		if idIdx >= len(args) {
			return nil, errors.New("syntax error")
		}

		key := args[keyIdx]
		idStr := args[idIdx]

		obj, exists := ctx.DB.Get(key)
		if !exists {
			continue
		}

		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm := strmVal.(*stream.Stream)

		var start string
		if idStr == "$" {
			lastID := strm.GetLastID()
			start = lastID.String()
		} else {
			start = idStr
		}

		entries := readEntriesAfter(strm, start, count)
		if len(entries) > 0 {
			results = append(results, formatStreamResult(key, entries))
		}
	}

	return command.NewArrayReply(results), nil
}

// XDEL deletes entries from a stream
func xdelCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewIntegerReply(0), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	ids := make([]stream.StreamID, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		id, err := stream.ParseStreamID(args[i])
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	deleted := strm.DeleteByID(ids)
	return command.NewIntegerReply(deleted), nil
}

// XTRIM trims a stream to a given size
func xtrimCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 4 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	strategy := strings.ToUpper(args[1])

	if strategy != "MAXLEN" {
		return nil, errors.New("unknown XTRIM option")
	}

	argIdx := 2
	if args[argIdx] == "~" {
		argIdx++
	}

	if argIdx >= len(args) {
		return nil, errors.New("syntax error")
	}

	maxLen, err := strconv.ParseInt(args[argIdx], 10, 64)
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewIntegerReply(0), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	currentLen := strm.Length()
	if currentLen <= maxLen {
		return command.NewIntegerReply(0), nil
	}

	entries := strm.GetEntries()
	if len(entries) == 0 {
		return command.NewIntegerReply(0), nil
	}

	cutoffIdx := int(currentLen - maxLen)
	minID := entries[cutoffIdx].ID
	maxID := entries[len(entries)-1].ID

	removed := strm.XTrim(minID, maxID)
	return command.NewIntegerReply(removed), nil
}

// XGROUP manages consumer groups
func xgroupCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	subcommand := strings.ToUpper(args[0])

	if len(args) < 2 {
		return nil, errors.New("syntax error")
	}

	// For CREATE: XGROUP CREATE key group id [MKSTREAM]
	// For DESTROY: XGROUP DESTROY key group
	// For CREATECONSUMER: XGROUP CREATECONSUMER key group consumer
	// For DELCONSUMER: XGROUP DELCONSUMER key group consumer

	key := args[1]
	var mkstream bool

	// Check for MKSTREAM option in CREATE command
	if subcommand == "CREATE" && len(args) >= 5 && strings.ToUpper(args[4]) == "MKSTREAM" {
		mkstream = true
	}

	obj, exists := ctx.DB.Get(key)
	var strm *stream.Stream
	if !exists {
		if subcommand == "CREATE" && mkstream {
			ctx.DB.Set(key, database.NewStreamObject())
			obj, _ = ctx.DB.Get(key)
			strmVal, _ := obj.GetStream()
			strm = strmVal.(*stream.Stream)
		} else {
			return nil, errors.New("No such key")
		}
	} else {
		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm = strmVal.(*stream.Stream)
	}

	switch subcommand {
	case "CREATE":
		if len(args) < 4 {
			return nil, errors.New("syntax error")
		}
		groupName := args[2]
		idStr := args[3]

		var initialID stream.StreamID
		if idStr == "$" {
			initialID = strm.GetLastID()
		} else if idStr == "0" {
			// "0" means "0-0"
			initialID = stream.StreamID{Timestamp: 0, Sequence: 0}
		} else {
			var err error
			initialID, err = stream.ParseStreamID(idStr)
			if err != nil {
				return nil, fmt.Errorf("Invalid stream ID: %w", err)
			}
		}

		cgroups := strm.GetConsumerGroupManager()
		if err := cgroups.CreateGroup(groupName, initialID); err != nil {
			return nil, err
		}
		return command.NewStatusReply("OK"), nil

	case "DESTROY":
		if len(args) < 3 {
			return nil, errors.New("syntax error")
		}
		groupName := args[2]
		cgroups := strm.GetConsumerGroupManager()
		if cgroups.DeleteGroup(groupName) {
			return command.NewIntegerReply(1), nil
		}
		return command.NewIntegerReply(0), nil

	case "CREATECONSUMER":
		if len(args) < 4 {
			return nil, errors.New("syntax error")
		}
		groupName := args[2]
		consumerName := args[3]

		cgroups := strm.GetConsumerGroupManager()
		group, ok := cgroups.GetGroup(groupName)
		if !ok {
			return nil, errors.New("No such group")
		}

		consumer := group.GetOrCreateConsumer(consumerName)
		if consumer != nil {
			return command.NewIntegerReply(1), nil
		}
		return command.NewIntegerReply(0), nil

	case "DELCONSUMER":
		if len(args) < 4 {
			return nil, errors.New("syntax error")
		}
		groupName := args[2]
		consumerName := args[3]

		cgroups := strm.GetConsumerGroupManager()
		group, ok := cgroups.GetGroup(groupName)
		if !ok {
			return nil, errors.New("No such group")
		}

		group.RemoveConsumer(consumerName)
		return command.NewIntegerReply(1), nil

	default:
		return nil, fmt.Errorf("unknown subcommand '%s'", subcommand)
	}
}

// XREADGROUP reads from a stream as a consumer group
func xreadgroupCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 5 {
		return nil, errors.New("wrong number of arguments")
	}

	if strings.ToUpper(args[0]) != "GROUP" {
		return nil, errors.New("syntax error")
	}

	groupName := args[1]
	consumerName := args[2]

	count := int64(1)

	idx := 3
	for idx < len(args) {
		arg := strings.ToUpper(args[idx])
		if arg == "COUNT" {
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			c, err := strconv.ParseInt(args[idx+1], 10, 64)
			if err != nil {
				return nil, errors.New("value is not an integer or out of range")
			}
			count = c
			idx += 2
		} else if arg == "BLOCK" {
			// Blocking not implemented yet
			if idx+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			idx += 2
		} else if arg == "STREAMS" {
			idx++
			break
		} else {
			idx++
		}
	}

	streamsIdx := -1
	for i := idx; i < len(args); i++ {
		if strings.ToUpper(args[i]) == "STREAMS" {
			streamsIdx = i + 1
			break
		}
	}

	if streamsIdx == -1 || streamsIdx >= len(args) {
		return nil, errors.New("syntax error")
	}

	streamCount := (len(args) - streamsIdx) / 2
	if streamCount == 0 {
		return nil, errors.New("syntax error")
	}

	results := make([]*command.Reply, 0)

	for i := 0; i < streamCount; i++ {
		keyIdx := streamsIdx + i
		idIdx := streamsIdx + streamCount + i

		if idIdx >= len(args) {
			return nil, errors.New("syntax error")
		}

		key := args[keyIdx]
		idStr := args[idIdx]

		obj, exists := ctx.DB.Get(key)
		if !exists {
			continue
		}

		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm := strmVal.(*stream.Stream)

		cgroups := strm.GetConsumerGroupManager()
		group, ok := cgroups.GetGroup(groupName)
		if !ok {
			return nil, errors.New("No such group")
		}

		_ = group.GetOrCreateConsumer(consumerName)

		var startID stream.StreamID
		var err error
		if idStr == ">" {
			startID = group.GetLastID()
		} else if idStr == "0" {
			startID = stream.StreamID{Timestamp: 0, Sequence: 0}
		} else {
			startID, err = stream.ParseStreamID(idStr)
			if err != nil {
				return nil, fmt.Errorf("Invalid stream ID: %w", err)
			}
		}

		entries := readEntriesAfter(strm, startID.String(), count)

		if len(entries) > 0 {
			newLastID := entries[len(entries)-1].ID
			group.SetLastID(newLastID)

			for _, entry := range entries {
				group.AddPendingID(consumerName, entry.ID, 0)
			}

			results = append(results, formatStreamResult(key, entries))
		}
	}

	return command.NewArrayReply(results), nil
}

// XACK acknowledges a message as processed
func xackCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	groupName := args[1]

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewIntegerReply(0), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	cgroups := strm.GetConsumerGroupManager()
	group, ok := cgroups.GetGroup(groupName)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	acknowledged := 0
	for i := 2; i < len(args); i++ {
		id, err := stream.ParseStreamID(args[i])
		if err != nil {
			continue
		}

		for _, consumer := range group.GetConsumers() {
			consumer.RemovePendingID(id)
		}
		acknowledged++
	}

	return command.NewIntegerReply(int64(acknowledged)), nil
}

// XCLAIM claims pending messages
func xclaimCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 5 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	groupName := args[1]
	consumerName := args[2]
	minIdleTime := args[3]

	_, err := strconv.ParseInt(minIdleTime, 10, 64)
	if err != nil {
		return nil, errors.New("value is not an integer or out of range")
	}

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return command.NewArrayReply(nil), nil
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	cgroups := strm.GetConsumerGroupManager()
	group, ok := cgroups.GetGroup(groupName)
	if !ok {
		return nil, errors.New("No such group")
	}

	ids := make([]stream.StreamID, 0)
	for i := 4; i < len(args); i++ {
		id, err := stream.ParseStreamID(args[i])
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	results := make([]*command.Reply, 0)
	_ = group.GetOrCreateConsumer(consumerName)

	for _, id := range ids {
		entry := strm.FindByID(id)
		if entry != nil {
			group.AddPendingID(consumerName, id, 0)
			results = append(results, formatStreamEntry(entry))
		}
	}

	return command.NewArrayReply(results), nil
}

// XPENDING shows pending messages
func xpendingCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := args[0]
	groupName := args[1]

	obj, exists := ctx.DB.Get(key)
	if !exists {
		return nil, errors.New("No such key")
	}

	strmVal, ok := obj.GetStream()
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	strm := strmVal.(*stream.Stream)

	cgroups := strm.GetConsumerGroupManager()
	group, ok := cgroups.GetGroup(groupName)
	if !ok {
		return nil, errors.New("No such group")
	}

	if len(args) == 3 {
		totalPending := 0
		smallestID := stream.StreamID{}
		largestID := stream.StreamID{}

		consumers := group.GetConsumers()
		consumerCount := len(consumers)

		for _, consumer := range consumers {
			pending := consumer.GetPendingIDs()
			totalPending += len(pending)
		}

		consumerInfo := make([]*command.Reply, 0, consumerCount)
		for name, consumer := range consumers {
			pendingCount := len(consumer.GetPendingIDs())
			consumerInfo = append(consumerInfo, command.NewArrayReply([]*command.Reply{
				command.NewBulkStringReply(name),
				command.NewIntegerReply(int64(pendingCount)),
			}))
		}

		result := []*command.Reply{
			command.NewIntegerReply(int64(totalPending)),
			command.NewBulkStringReply(smallestID.String()),
			command.NewBulkStringReply(largestID.String()),
			command.NewArrayReply(consumerInfo),
		}

		return command.NewArrayReply(result), nil
	}

	return nil, errors.New("detailed pending view not yet implemented")
}

// XINFO provides information about streams and consumer groups
func xinfoCmd(ctx *command.Context) (*command.Reply, error) {
	args := ctx.Args
	if len(args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	subcommand := strings.ToUpper(args[0])

	switch subcommand {
	case "STREAMS":
		return command.NewArrayReply(nil), nil

	case "STREAM":
		if len(args) < 2 {
			return nil, errors.New("syntax error")
		}
		key := args[1]

		obj, exists := ctx.DB.Get(key)
		if !exists {
			return nil, errors.New("No such key")
		}

		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm := strmVal.(*stream.Stream)

		cgroups := strm.GetConsumerGroupManager()
		groups := cgroups.GetGroups()

		return command.NewArrayReply([]*command.Reply{
			command.NewBulkStringReply("length"),
			command.NewIntegerReply(strm.Length()),
			command.NewBulkStringReply("groups"),
			command.NewIntegerReply(int64(len(groups))),
			command.NewBulkStringReply("last-generated-id"),
			command.NewBulkStringReply(strm.GetLastID().String()),
		}), nil

	case "GROUPS":
		if len(args) < 2 {
			return nil, errors.New("syntax error")
		}
		key := args[1]

		obj, exists := ctx.DB.Get(key)
		if !exists {
			return command.NewArrayReply(nil), nil
		}

		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm := strmVal.(*stream.Stream)

		cgroups := strm.GetConsumerGroupManager()
		groups := cgroups.GetGroups()

		result := make([]*command.Reply, 0, len(groups))
		for _, group := range groups {
			groupInfo := []*command.Reply{
				command.NewBulkStringReply("name"),
				command.NewBulkStringReply(group.GetName()),
				command.NewBulkStringReply("last-delivered-id"),
				command.NewBulkStringReply(group.GetLastID().String()),
				command.NewBulkStringReply("consumers"),
				command.NewIntegerReply(int64(len(group.GetConsumers()))),
			}
			result = append(result, command.NewArrayReply(groupInfo))
		}

		return command.NewArrayReply(result), nil

	case "CONSUMERS":
		if len(args) < 3 {
			return nil, errors.New("syntax error")
		}
		key := args[1]
		groupName := args[2]

		obj, exists := ctx.DB.Get(key)
		if !exists {
			return nil, errors.New("No such key")
		}

		strmVal, ok := obj.GetStream()
		if !ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		strm := strmVal.(*stream.Stream)

		cgroups := strm.GetConsumerGroupManager()
		group, ok := cgroups.GetGroup(groupName)
		if !ok {
			return nil, errors.New("No such group")
		}

		consumers := group.GetConsumers()
		result := make([]*command.Reply, 0, len(consumers))

		for _, consumer := range consumers {
			pendingCount := len(consumer.GetPendingIDs())
			consumerInfo := []*command.Reply{
				command.NewBulkStringReply("name"),
				command.NewBulkStringReply(consumer.GetName()),
				command.NewBulkStringReply("pending"),
				command.NewIntegerReply(int64(pendingCount)),
			}
			result = append(result, command.NewArrayReply(consumerInfo))
		}

		return command.NewArrayReply(result), nil

	default:
		return nil, fmt.Errorf("unknown subcommand '%s'", subcommand)
	}
}

// Helper functions

func formatStreamEntries(entries []*stream.StreamEntry) *command.Reply {
	if entries == nil {
		return command.NewArrayReply(nil)
	}

	result := make([]*command.Reply, len(entries))
	for i, entry := range entries {
		result[i] = formatStreamEntry(entry)
	}

	return command.NewArrayReply(result)
}

func formatStreamEntry(entry *stream.StreamEntry) *command.Reply {
	fields := entry.GetFields()

	fieldArray := make([]*command.Reply, 0, len(fields)*2)
	for k, v := range fields {
		fieldArray = append(fieldArray, command.NewBulkStringReply(k))
		fieldArray = append(fieldArray, command.NewBulkStringReply(v))
	}

	return command.NewArrayReply([]*command.Reply{
		command.NewBulkStringReply(entry.ID.String()),
		command.NewArrayReply(fieldArray),
	})
}

func formatStreamResult(key string, entries []*stream.StreamEntry) *command.Reply {
	return command.NewArrayReply([]*command.Reply{
		command.NewBulkStringReply(key),
		formatStreamEntries(entries),
	})
}

func readEntriesAfter(strm *stream.Stream, startID string, count int64) []*stream.StreamEntry {
	var start stream.StreamID
	var err error

	if startID == "0" || startID == "0-0" {
		start = stream.StreamID{}
	} else {
		start, err = stream.ParseStreamID(startID)
		if err != nil {
			return nil
		}
	}

	if start.IsZero() {
		return strm.Range("-", "+", count)
	}

	entries := strm.GetEntries()
	startIdx := -1

	for i, entry := range entries {
		if entry.ID.Compare(start) > 0 {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		return nil
	}

	endIdx := len(entries) - 1
	if count > 0 {
		endIdx = startIdx + int(count) - 1
		if endIdx >= len(entries) {
			endIdx = len(entries) - 1
		}
	}

	if startIdx > endIdx {
		return nil
	}

	result := make([]*stream.StreamEntry, 0, endIdx-startIdx+1)
	for i := startIdx; i <= endIdx && i < len(entries); i++ {
		result = append(result, entries[i])
	}

	return result
}
