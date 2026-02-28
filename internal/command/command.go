// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package command

import (
	"fmt"

	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/net"
	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// Context represents the command execution context
type Context struct {
	DB      *database.DB
	Conn    *net.Conn
	CmdName string
	Args    []string
}

// Handler is the command handler function
type Handler func(ctx *Context) (*Reply, error)

// Command represents a Redis command
type Command struct {
	Name             string
	Handler          Handler
	Arity            int      // Number of arguments (negative for >= -n, 0 for exact or optional)
	Flags            []string // Command flags
	FirstKey         int      // Index of first key
	LastKey          int      // Index of last key
	StepCount        int      // Step count for key scanning
	Categories       []string // Command categories
	OptionalFirstArg bool     // Allow 0 arguments when Arity is negative
}

const (
	// Command flags
	FlagReadOnly      = "readonly"
	FlagWrite         = "write"
	FlagDenyOOM       = "denyoom"
	FlagAdmin         = "admin"
	FlagPubSub        = "pubsub"
	FlagNoScript      = "noscript"
	FlagRandom        = "random"
	FlagSortForScript = "sort_for_script"
	FlagLoading       = "loading"
	FlagStale         = "stale"
	FlagSkipMonitor   = "skip_monitor"
	FlagSkipSlowlog   = "skip_slowlog"
	FlagFast          = "fast"
	FlagNoAuth        = "no_auth"
	FlagMayReplicate  = "may_replicate"
)

// Category constants
const (
	CatString      = "string"
	CatList        = "list"
	CatSet         = "set"
	CatHash        = "hash"
	CatZSet        = "sorted_set"
	CatStream      = "stream"
	CatPubSub      = "pubsub"
	CatTransaction = "transaction"
	CatConnection  = "connection"
	CatServer      = "server"
	CatKey         = "key"
	CatGeneric     = "generic"
	CatHyperLogLog = "hyperloglog"
	CatGeo         = "geo"
	CatPersistence = "persistence"
	CatFast        = "fast"
	CatKeySpace    = "keyspace"
	CatScript      = "script"
)

// Reply represents a command reply
type Reply struct {
	Type  ReplyType
	Value interface{}
}

// ReplyType represents the type of reply
type ReplyType int

const (
	ReplyTypeStatus ReplyType = iota
	ReplyTypeError
	ReplyTypeInteger
	ReplyTypeBulkString
	ReplyTypeArray
	ReplyTypeNil
)

// NewStatusReply creates a status reply
func NewStatusReply(status string) *Reply {
	return &Reply{
		Type:  ReplyTypeStatus,
		Value: status,
	}
}

// NewErrorReply creates an error reply
func NewErrorReply(err error) *Reply {
	return &Reply{
		Type:  ReplyTypeError,
		Value: err.Error(),
	}
}

// NewErrorReplyStr creates an error reply from string
func NewErrorReplyStr(err string) *Reply {
	return &Reply{
		Type:  ReplyTypeError,
		Value: err,
	}
}

// NewIntegerReply creates an integer reply
func NewIntegerReply(i int64) *Reply {
	return &Reply{
		Type:  ReplyTypeInteger,
		Value: i,
	}
}

// NewBulkStringReply creates a bulk string reply
func NewBulkStringReply(s string) *Reply {
	return &Reply{
		Type:  ReplyTypeBulkString,
		Value: s,
	}
}

// NewBulkStringReplyBytes creates a bulk string reply from bytes
func NewBulkStringReplyBytes(b []byte) *Reply {
	return &Reply{
		Type:  ReplyTypeBulkString,
		Value: b,
	}
}

// NewArrayReply creates an array reply
func NewArrayReply(items []*Reply) *Reply {
	return &Reply{
		Type:  ReplyTypeArray,
		Value: items,
	}
}

// NewStringArrayReply creates an array reply from strings
func NewStringArrayReply(items []string) *Reply {
	return &Reply{
		Type:  ReplyTypeArray,
		Value: items,
	}
}

// NewNilReply creates a nil reply
func NewNilReply() *Reply {
	return &Reply{
		Type: ReplyTypeNil,
	}
}

// NewArrayReplyFromAny creates an array reply from interface{} slice
func NewArrayReplyFromAny(items []interface{}) *Reply {
	return &Reply{
		Type:  ReplyTypeArray,
		Value: items,
	}
}

// IsNil returns true if the reply is nil
func (r *Reply) IsNil() bool {
	return r == nil || r.Type == ReplyTypeNil
}

// IsError returns true if the reply is an error
func (r *Reply) IsError() bool {
	return r != nil && r.Type == ReplyTypeError
}

// Marshal converts the reply to RESP bytes
func (r *Reply) Marshal() []byte {
	if r == nil {
		return resp.BuildNil()
	}

	switch r.Type {
	case ReplyTypeStatus:
		if s, ok := r.Value.(string); ok {
			return resp.BuildSimpleString(s)
		}
		return resp.BuildOK()
	case ReplyTypeError:
		return resp.BuildErrorString(r.Value.(string))
	case ReplyTypeInteger:
		return resp.BuildInteger(r.Value.(int64))
	case ReplyTypeBulkString:
		switch v := r.Value.(type) {
		case string:
			return resp.BuildBulkString(v)
		case []byte:
			return resp.BuildBulkStringBytes(v)
		default:
			return resp.BuildBulkString(fmt.Sprintf("%v", v))
		}
	case ReplyTypeArray:
		switch v := r.Value.(type) {
		case []*Reply:
			if len(v) == 0 {
				return resp.BuildEmptyArray()
			}
			builder := resp.NewResponseBuilder()
			builder.WriteArray(len(v))
			for _, item := range v {
				builder.WriteBytes(item.Marshal())
			}
			return builder.Bytes()
		case []string:
			return resp.BuildStringArray(v)
		case []interface{}:
			if len(v) == 0 {
				return resp.BuildEmptyArray()
			}
			builder := resp.NewResponseBuilder()
			builder.WriteArray(len(v))
			for _, item := range v {
				switch val := item.(type) {
				case nil:
					builder.WriteBytes(resp.BuildNil())
				case string:
					builder.WriteBulkStringFromString(val)
				case int64:
					builder.WriteInteger(val)
				case int:
					builder.WriteInteger(int64(val))
				default:
					builder.WriteBulkStringFromString(fmt.Sprintf("%v", val))
				}
			}
			return builder.Bytes()
		default:
			return resp.BuildEmptyArray()
		}
	case ReplyTypeNil:
		return resp.BuildNil()
	default:
		return resp.BuildErrorString("ERR unknown reply type")
	}
}

// HasFlag checks if the command has a specific flag
func (c *Command) HasFlag(flag string) bool {
	for _, f := range c.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

// CheckArity checks if the command has the correct number of arguments
func (c *Command) CheckArity(argc int) error {
	// Arity in Redis includes the command name, but our argc doesn't
	// So we need to adjust: expected args = Arity - 1 (or -Arity - 1 for negative)
	arity := c.Arity

	if arity > 0 {
		// Exact number of arguments required
		expected := arity - 1
		if argc != expected {
			return fmt.Errorf("wrong number of arguments for '%s' command", c.Name)
		}
	} else if arity < 0 {
		// At least -arity arguments required (minimum)
		minArgs := -arity - 1
		// If OptionalFirstArg is set, allow 0 args when minArgs is 0
		if c.OptionalFirstArg && minArgs == 0 && argc == 0 {
			return nil
		}
		if argc < minArgs {
			return fmt.Errorf("wrong number of arguments for '%s' command", c.Name)
		}
	}
	// arity == 0 means no arguments (or variable number of arguments)

	return nil
}

// GetKeys extracts the keys from the command arguments
func (c *Command) GetKeys(args []string) []string {
	if c.FirstKey < 0 || c.LastKey < 0 {
		return nil
	}

	if c.StepCount <= 0 {
		start := c.FirstKey
		end := c.LastKey
		if end >= len(args) {
			end = len(args) - 1
		}
		if start >= len(args) {
			return nil
		}
		if end < start {
			return nil
		}
		return args[start : end+1]
	}

	// For commands with step count (like MSET)
	keys := []string{}
	for i := c.FirstKey; i < len(args) && i <= c.LastKey; i += c.StepCount {
		keys = append(keys, args[i])
	}
	return keys
}
