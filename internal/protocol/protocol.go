// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package protocol

import "io"

// Message represents a protocol message
type Message interface {
	// Type returns the message type
	Type() MessageType

	// Marshal serializes the message to bytes
	Marshal() []byte
}

// MessageType represents the type of a protocol message
type MessageType int

const (
	TypeSimpleString MessageType = iota
	TypeError
	TypeInteger
	TypeBulkString
	TypeArray
)

// Parser is the interface for parsing protocol messages
type Parser interface {
	// Parse reads and parses a single message
	Parse() (Message, error)

	// ParseCommand parses a command (name and arguments)
	ParseCommand() (string, []string, error)
}

// Serializer is the interface for serializing protocol messages
type Serializer interface {
	// Write writes raw data
	Write(data []byte) error

	// WriteMessage writes a complete message
	WriteMessage(msg Message) error

	// Flush flushes any buffered data
	Flush() error
}

// Conn represents a connection that uses the protocol
type Conn interface {
	io.Reader
	io.Writer

	// SetReadBuffer sets the read buffer size
	SetReadBuffer(size int)

	// SetWriteBuffer sets the write buffer size
	SetWriteBuffer(size int)

	// Close closes the connection
	Close() error
}
