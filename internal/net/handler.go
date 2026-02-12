// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// DefaultHandler is the default connection handler
type DefaultHandler struct {
	processor CommandProcessor
}

// CommandProcessor processes commands
type CommandProcessor interface {
	// ProcessCommand processes a command and returns the response
	ProcessCommand(ctx context.Context, conn *Conn, cmd string, args []string) ([]byte, error)
}

// NewDefaultHandler creates a new default handler
func NewDefaultHandler(processor CommandProcessor) *DefaultHandler {
	return &DefaultHandler{
		processor: processor,
	}
}

// Handle handles a connection
func (h *DefaultHandler) Handle(ctx context.Context, conn *Conn) {
	parser := conn.NewRESPParser()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline for blocking read
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Second))

		// Parse command
		msg, err := parser.Parse()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if IsConnectionClosed(err) {
				return
			}
			// Send error response
			_ = conn.WriteRESP(resp.BuildErrorString(fmt.Sprintf("ERR protocol error: %s", err.Error())))
			_ = conn.Flush()
			return
		}

		// Parse command name and arguments
		cmdName, args, err := msg.ParseCommand()
		if err != nil {
			_ = conn.WriteRESP(resp.BuildErrorString(fmt.Sprintf("ERR parsing error: %s", err.Error())))
			_ = conn.Flush()
			continue
		}

		// Check for QUIT command
		if cmdName == "QUIT" {
			_ = conn.WriteRESP(resp.BuildOK())
			_ = conn.Flush()
			return
		}

		// Process command
		response, err := h.processor.ProcessCommand(ctx, conn, cmdName, args)
		if err != nil && !resp.IsError(response) {
			response = resp.BuildError(err)
		}
		_ = conn.WriteRESP(response)

		// Flush response
		if err := conn.Flush(); err != nil {
			return
		}
	}
}

// IsConnectionClosed checks if an error indicates connection is closed
func IsConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	// Check for common connection closed errors
	errStr := err.Error()
	return contains(errStr, "closed") ||
		contains(errStr, "reset by peer") ||
		contains(errStr, "broken pipe") ||
		contains(errStr, "connection reset")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// DefaultHandle is a convenience function to handle connections with a CommandProcessor
func DefaultHandle(ctx context.Context, conn *Conn, processor CommandProcessor) {
	handler := &DefaultHandler{processor: processor}
	handler.Handle(ctx, conn)
}
