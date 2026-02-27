// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/pubsub"
)

var (
	pubsubMgr *pubsub.Manager
)

// SetPubSubManager sets the global pubsub manager
func SetPubSubManager(mgr *pubsub.Manager) {
	pubsubMgr = mgr
}

// RegisterPubSubCommands registers all pub/sub commands
func RegisterPubSubCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "PUBLISH",
		Handler:    publishCmd,
		Arity:      3,
		Flags:      []string{command.FlagPubSub, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})

	disp.Register(&command.Command{
		Name:       "SUBSCRIBE",
		Handler:    subscribeCmd,
		Arity:      -2,
		Flags:      []string{command.FlagPubSub, command.FlagReadOnly},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})

	disp.Register(&command.Command{
		Name:       "UNSUBSCRIBE",
		Handler:    unsubscribeCmd,
		Arity:      -1,
		Flags:      []string{command.FlagPubSub, command.FlagReadOnly},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})

	disp.Register(&command.Command{
		Name:       "PSUBSCRIBE",
		Handler:    psubscribeCmd,
		Arity:      -2,
		Flags:      []string{command.FlagPubSub, command.FlagReadOnly},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})

	disp.Register(&command.Command{
		Name:       "PUNSUBSCRIBE",
		Handler:    punsubscribeCmd,
		Arity:      -1,
		Flags:      []string{command.FlagPubSub, command.FlagReadOnly},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})

	disp.Register(&command.Command{
		Name:       "PUBSUB",
		Handler:    pubsubCmd,
		Arity:      -2,
		Flags:      []string{command.FlagPubSub, command.FlagReadOnly, command.FlagFast},
		FirstKey:   0,
		LastKey:    0,
		Categories: []string{command.CatPubSub},
	})
}

// PUBLISH channel message
func publishCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'PUBLISH' command"), nil
	}

	channel := ctx.Args[0]
	message := ctx.Args[1]

	count := pubsubMgr.Publish(channel, []byte(message))
	return command.NewIntegerReply(int64(count)), nil
}

// SUBSCRIBE channel [channel ...]
func subscribeCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'SUBSCRIBE' command"), nil
	}

	channels := ctx.Args
	pubsubMgr.Subscribe(ctx.Conn, channels...)

	// For Redis compatibility, each subscription confirmation is a separate 3-element array
	// Since we can only return one Reply, we'll return the first channel's confirmation
	// In a full implementation, we'd need to push additional messages for other channels
	subCount := len(ctx.Conn.GetSubscriptions())
	channel := channels[0]

	// Return a flat 3-element array: ["subscribe", "channel", count]
	response := []interface{}{"subscribe", channel, int64(subCount)}
	return command.NewArrayReplyFromAny(response), nil
}

// UNSUBSCRIBE [channel ...]
func unsubscribeCmd(ctx *command.Context) (*command.Reply, error) {
	var channels []string

	if len(ctx.Args) == 0 {
		// Get current subscriptions to unsubscribe from all
		currentSubs := ctx.Conn.GetSubscriptions()
		channels = make([]string, 0, len(currentSubs))
		for channel := range currentSubs {
			channels = append(channels, channel)
		}
	} else {
		channels = ctx.Args
	}

	if len(channels) == 0 {
		// No subscriptions, return empty array-like response
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	// Build response with unsubscribe confirmation messages
	responses := make([]interface{}, 0, len(channels)*3)

	subCount := len(ctx.Conn.GetSubscriptions())
	for _, channel := range channels {
		_, isSubscribed := ctx.Conn.GetSubscriptions()[channel]
		if isSubscribed {
			subCount--
		}
		responses = append(responses, []interface{}{"unsubscribe", channel, int64(subCount)})
	}

	pubsubMgr.Unsubscribe(ctx.Conn, channels...)

	return command.NewArrayReplyFromAny(responses), nil
}

// PSUBSCRIBE pattern [pattern ...]
func psubscribeCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'PSUBSCRIBE' command"), nil
	}

	patterns := ctx.Args
	pubsubMgr.PSubscribe(ctx.Conn, patterns...)

	// Build response array with psubscribe confirmation messages
	responses := make([]interface{}, 0, len(patterns)*3)

	patCount := len(ctx.Conn.GetPatterns())
	for _, pattern := range patterns {
		patCount++
		responses = append(responses, []interface{}{"psubscribe", pattern, patCount})
	}

	return command.NewArrayReplyFromAny(responses), nil
}

// PUNSUBSCRIBE [pattern ...]
func punsubscribeCmd(ctx *command.Context) (*command.Reply, error) {
	var patterns []string

	if len(ctx.Args) == 0 {
		// Get current pattern subscriptions to unsubscribe from all
		currentPats := ctx.Conn.GetPatterns()
		patterns = make([]string, 0, len(currentPats))
		for pattern := range currentPats {
			patterns = append(patterns, pattern)
		}
	} else {
		patterns = ctx.Args
	}

	if len(patterns) == 0 {
		// No pattern subscriptions, return empty response
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	// Build response with punsubscribe confirmation messages
	responses := make([]interface{}, 0, len(patterns)*3)

	patCount := len(ctx.Conn.GetPatterns())
	for _, pattern := range patterns {
		_, isSubscribed := ctx.Conn.GetPatterns()[pattern]
		if isSubscribed {
			patCount--
		}
		responses = append(responses, []interface{}{"punsubscribe", pattern, int64(patCount)})
	}

	pubsubMgr.PUnsubscribe(ctx.Conn, patterns...)

	return command.NewArrayReplyFromAny(responses), nil
}

// PUBSUB subcommand [argument [argument ...]]
func pubsubCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) == 0 {
		return command.NewErrorReplyStr("ERR wrong number of arguments for 'PUBSUB' command"), nil
	}

	subcommand := strings.ToLower(ctx.Args[0])

	switch subcommand {
	case "channels":
		return pubsubChannels(ctx)
	case "numsub":
		return pubsubNumsub(ctx)
	case "numpat":
		return pubsubNumpat(ctx)
	default:
		return command.NewErrorReplyStr(fmt.Sprintf("ERR unknown PUBSUB subcommand '%s'", subcommand)), nil
	}
}

// PUBSUB CHANNELS [pattern]
func pubsubChannels(ctx *command.Context) (*command.Reply, error) {
	// Get all active channels
	channels := pubsubMgr.ListChannels()

	// If pattern is provided, filter channels
	if len(ctx.Args) > 1 {
		pattern := ctx.Args[1]
		filtered := make([]string, 0)
		for _, channel := range channels {
			matched, _ := matchPatternSimple(pattern, channel)
			if matched {
				filtered = append(filtered, channel)
			}
		}
		return command.NewStringArrayReply(filtered), nil
	}

	return command.NewStringArrayReply(channels), nil
}

// PUBSUB NUMSUB [channel [channel ...]]
func pubsubNumsub(ctx *command.Context) (*command.Reply, error) {
	var channels []string

	if len(ctx.Args) > 1 {
		channels = ctx.Args[1:]
	} else {
		channels = pubsubMgr.ListChannels()
	}

	if len(channels) == 0 {
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	numSubs := pubsubMgr.NumSubscribers(channels...)

	// Build response: ["channel1", "1", "channel2", "2", ...]
	result := make([]interface{}, 0, len(channels)*2)
	for _, channel := range channels {
		result = append(result, channel, int64(numSubs[channel]))
	}

	return command.NewArrayReplyFromAny(result), nil
}

// PUBSUB NUMPAT
func pubsubNumpat(ctx *command.Context) (*command.Reply, error) {
	return command.NewIntegerReply(int64(pubsubMgr.NumPatterns())), nil
}

// matchPatternSimple checks if a string matches a simple glob pattern
// Supports * wildcard only
func matchPatternSimple(pattern, s string) (bool, error) {
	if pattern == "*" {
		return true, nil
	}

	// Simple wildcard matching
	patternParts := strings.Split(pattern, "*")
	if len(patternParts) == 1 {
		return pattern == s, nil
	}

	// Check if string starts with first part
	if !strings.HasPrefix(s, patternParts[0]) {
		return false, nil
	}

	// Check if string ends with last part (if not empty due to trailing *)
	if patternParts[len(patternParts)-1] != "" {
		if !strings.HasSuffix(s, patternParts[len(patternParts)-1]) {
			return false, nil
		}
	}

	return true, nil
}

// BuildSubscribeMessage builds a RESP message for subscribe/punsubscribe confirmation
func BuildSubscribeMessage(action string, target string, count int) []byte {
	// Format: *3\r\n$9\r\nsubscribe\r\n$7\r\ntarget\r\n:1\r\n
	var builder strings.Builder
	builder.WriteString("*3\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(action)))
	builder.WriteString("\r\n")
	builder.WriteString(action)
	builder.WriteString("\r\n")
	builder.WriteString("$")
	builder.WriteString(strconv.Itoa(len(target)))
	builder.WriteString("\r\n")
	builder.WriteString(target)
	builder.WriteString("\r\n")
	builder.WriteString(":")
	builder.WriteString(strconv.Itoa(count))
	builder.WriteString("\r\n")
	return []byte(builder.String())
}
