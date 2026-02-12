// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package command

import (
	"fmt"
	"strconv"

	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// StatusOK returns a simple "OK" response
func StatusOK() []byte {
	return resp.BuildOK()
}

// StatusPong returns a simple "PONG" response
func StatusPong() []byte {
	return resp.BuildPong()
}

// StatusQueued returns a simple "QUEUED" response
func StatusQueued() []byte {
	return resp.BuildQueued()
}

// NilReply returns a nil bulk string
func NilReply() []byte {
	return resp.BuildNil()
}

// IntegerReply returns an integer response
func IntegerReply(i int64) []byte {
	return resp.BuildInteger(i)
}

// BulkStringReply returns a bulk string response
func BulkStringReply(s string) []byte {
	return resp.BuildBulkString(s)
}

// ErrorReply returns an error response
func ErrorReply(err error) []byte {
	return resp.BuildError(err)
}

// ErrorReplyStr returns an error response from string
func ErrorReplyStr(err string) []byte {
	return resp.BuildErrorString(err)
}

// ArrayReply returns an array response
func ArrayReply(items ...[]byte) []byte {
	return resp.BuildBulkStringArray(items)
}

// StringArrayReply returns an array of strings response
func StringArrayReply(items []string) []byte {
	return resp.BuildStringArray(items)
}

// IntegerArrayReply returns an array of integers response
func IntegerArrayReply(items []int64) []byte {
	builder := resp.NewResponseBuilder()
	builder.WriteArray(len(items))
	for _, item := range items {
		builder.WriteInteger(item)
	}
	return builder.Bytes()
}

// MultiBulkReply returns a multi bulk reply from a variadic list of strings
func MultiBulkReply(items ...string) []byte {
	if len(items) == 0 {
		return resp.BuildEmptyArray()
	}

	builder := resp.NewResponseBuilder()
	builder.WriteArray(len(items))
	for _, item := range items {
		builder.WriteBulkStringFromString(item)
	}
	return builder.Bytes()
}

// BuildSimpleError builds a simple error message
func BuildSimpleError(msg string) []byte {
	return resp.BuildErrorString("ERR " + msg)
}

// BuildWrongArgcError builds a wrong number of arguments error
func BuildWrongArgcError(cmd string) []byte {
	return resp.BuildErrorString(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd))
}

// BuildTypeError builds a type error
func BuildTypeError(msg string) []byte {
	return resp.BuildErrorString("WRONGTYPE " + msg)
}

// BuildSyntaxError builds a syntax error
func BuildSyntaxError(msg string) []byte {
	return resp.BuildErrorString("SYNTAX ERROR " + msg)
}

// UnknownCommandError returns an error for unknown command
func UnknownCommandError(cmd string) []byte {
	return resp.BuildErrorString(fmt.Sprintf("ERR unknown command '%s'", cmd))
}

// BuildFloatReply builds a float reply as bulk string
func BuildFloatReply(f float64) []byte {
	return resp.BuildBulkString(strconv.FormatFloat(f, 'f', -1, 64))
}
