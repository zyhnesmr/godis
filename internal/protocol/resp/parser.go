// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	ErrInvalidSyntax    = errors.New("invalid syntax")
	ErrInvalidType      = errors.New("invalid type")
	ErrInvalidLength    = errors.New("invalid length")
	ErrIncomplete       = errors.New("incomplete message")
	ErrCRLFExpected     = errors.New("CRLF expected")
	ErrBulkStringTooBig = errors.New("bulk string too big")
)

const (
	maxBulkStringSize = 512 * 1024 * 1024 // 512MB
)

// Parser parses RESP protocol messages
type Parser struct {
	reader *bufio.Reader
}

// NewParser creates a new RESP parser
func NewParser(reader io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(reader),
	}
}

// ReadLine reads a line ending with \r\n
func (p *Parser) ReadLine() (string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", ErrCRLFExpected
	}
	return line[:len(line)-2], nil
}

// ReadBytes reads exactly n bytes from the reader
func (p *Parser) ReadBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrInvalidLength
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(p.reader, buf)
	if err != nil {
		return nil, err
	}
	// Read trailing \r\n
	crlf := make([]byte, 2)
	_, err = io.ReadFull(p.reader, crlf)
	if err != nil {
		return nil, err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, ErrCRLFExpected
	}
	return buf, nil
}

// Parse reads and parses a single RESP message
func (p *Parser) Parse() (*Message, error) {
	line, err := p.ReadLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrInvalidSyntax
	}

	msgType := Type(line[0])
	line = line[1:]

	switch msgType {
	case TypeSimpleString:
		return NewSimpleString(line), nil

	case TypeError:
		return NewError(line), nil

	case TypeInteger:
		i, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid integer: %s", ErrInvalidSyntax, line)
		}
		return NewInteger(i), nil

	case TypeBulkString:
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid bulk string length: %s", ErrInvalidSyntax, line)
		}
		if length < 0 {
			// Null bulk string
			return NewNilBulkString(), nil
		}
		if length > maxBulkStringSize {
			return nil, ErrBulkStringTooBig
		}
		data, err := p.ReadBytes(length)
		if err != nil {
			return nil, err
		}
		return NewBulkString(data), nil

	case TypeArray:
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid array length: %s", ErrInvalidSyntax, line)
		}
		if length < 0 {
			// Null array
			return NewArray(nil), nil
		}
		items := make([]*Message, length)
		for i := 0; i < length; i++ {
			item, err := p.Parse()
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return NewArray(items), nil

	default:
		return nil, fmt.Errorf("%w: unknown type: %c", ErrInvalidType, msgType)
	}
}

// ParseCommand parses a RESP array as a command
// Returns the command name and arguments
func (m *Message) ParseCommand() (string, []string, error) {
	if m.Type != TypeArray {
		return "", nil, fmt.Errorf("expected array, got %c", m.Type)
	}

	items, ok := m.Array()
	if !ok {
		return "", nil, errors.New("invalid array")
	}

	if len(items) == 0 {
		return "", nil, errors.New("empty array")
	}

	// Get command name from first item
	cmd, ok := items[0].String()
	if !ok {
		return "", nil, errors.New("command is not a string")
	}

	args := make([]string, 0, len(items)-1)
	for i := 1; i < len(items); i++ {
		arg, ok := items[i].String()
		if !ok {
			// Try to convert integer to string
			if intVal, ok := items[i].Integer(); ok {
				args = append(args, strconv.FormatInt(intVal, 10))
				continue
			}
			return "", nil, fmt.Errorf("argument %d is not a string", i)
		}
		args = append(args, arg)
	}

	return cmd, args, nil
}

// ParseCommandFromReader parses a command directly from a reader
func ParseCommandFromReader(r io.Reader) (string, []string, error) {
	parser := NewParser(r)
	msg, err := parser.Parse()
	if err != nil {
		return "", nil, err
	}
	return msg.ParseCommand()
}

// ParseMultipleCommands parses multiple commands from a reader
// This is useful for pipelining
func (p *Parser) ParseMultipleCommands() ([]*Message, error) {
	var commands []*Message

	for {
		line, err := p.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if len(line) == 0 {
			continue
		}

		msgType := Type(line[0])
		line = line[1:]

		switch msgType {
		case TypeSimpleString:
			commands = append(commands, NewSimpleString(line))

		case TypeError:
			commands = append(commands, NewError(line))

		case TypeInteger:
			i, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid integer: %s", ErrInvalidSyntax, line)
			}
			commands = append(commands, NewInteger(i))

		case TypeBulkString:
			length, err := strconv.Atoi(line)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid bulk string length: %s", ErrInvalidSyntax, line)
			}
			if length < 0 {
				commands = append(commands, NewNilBulkString())
				continue
			}
			if length > maxBulkStringSize {
				return nil, ErrBulkStringTooBig
			}
			data, err := p.ReadBytes(length)
			if err != nil {
				return nil, err
			}
			commands = append(commands, NewBulkString(data))

		case TypeArray:
			length, err := strconv.Atoi(line)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid array length: %s", ErrInvalidSyntax, line)
			}
			if length < 0 {
				commands = append(commands, NewArray(nil))
				continue
			}
			items := make([]*Message, length)
			for i := 0; i < length; i++ {
				item, err := p.Parse()
				if err != nil {
					return nil, err
				}
				items[i] = item
			}
			commands = append(commands, NewArray(items))

		default:
			return nil, fmt.Errorf("%w: unknown type: %c", ErrInvalidType, msgType)
		}
	}

	return commands, nil
}

// IsError returns true if the message is an error type
func IsError(msg []byte) bool {
	return len(msg) > 0 && msg[0] == '-'
}

// GetErrorMessage extracts the error message from an error RESP message
func GetErrorMessage(msg []byte) string {
	if !IsError(msg) {
		return ""
	}
	// Format: -ERROR message\r\n
	if len(msg) >= 2 && msg[len(msg)-2] == '\r' && msg[len(msg)-1] == '\n' {
		return string(msg[1 : len(msg)-2])
	}
	return string(msg[1:])
}

// ExtractCommandName extracts just the command name from RESP input
// without parsing the entire message
func ExtractCommandName(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// Array format: *<count>\r\n$<len>\r\n<command>\r\n
	if data[0] == '*' {
		// Find first bulk string
		idx := 0
		for i := 0; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				idx = i + 2
				break
			}
		}
		if idx >= len(data) || data[idx] != '$' {
			return ""
		}
		// Skip to the bulk string content
		for i := idx; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				idx = i + 2
				break
			}
		}
		// Extract command until \r
		end := idx
		for end < len(data) && data[end] != '\r' {
			end++
		}
		if end > idx {
			return string(data[idx:end])
		}
	}

	return ""
}
