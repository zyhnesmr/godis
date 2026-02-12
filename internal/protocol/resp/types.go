// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

// Type represents the RESP data type
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

// Message represents a RESP message
type Message struct {
	Type  Type
	Value interface{}
}

// NewSimpleString creates a simple string message
func NewSimpleString(s string) *Message {
	return &Message{Type: TypeSimpleString, Value: s}
}

// NewError creates an error message
func NewError(s string) *Message {
	return &Message{Type: TypeError, Value: s}
}

// NewInteger creates an integer message
func NewInteger(i int64) *Message {
	return &Message{Type: TypeInteger, Value: i}
}

// NewBulkString creates a bulk string message
func NewBulkString(s []byte) *Message {
	return &Message{Type: TypeBulkString, Value: s}
}

// NewNilBulkString creates a nil bulk string message
func NewNilBulkString() *Message {
	return &Message{Type: TypeBulkString, Value: nil}
}

// NewArray creates an array message
func NewArray(items []*Message) *Message {
	return &Message{Type: TypeArray, Value: items}
}

// IsNil returns true if the message represents a nil value
func (m *Message) IsNil() bool {
	return m.Type == TypeBulkString && m.Value == nil
}

// String returns the string representation of simple strings and bulk strings
func (m *Message) String() (string, bool) {
	switch m.Type {
	case TypeSimpleString:
		return m.Value.(string), true
	case TypeBulkString:
		if m.Value == nil {
			return "", false
		}
		return string(m.Value.([]byte)), true
	default:
		return "", false
	}
}

// Integer returns the integer value
func (m *Message) Integer() (int64, bool) {
	if m.Type == TypeInteger {
		return m.Value.(int64), true
	}
	return 0, false
}

// Array returns the array value
func (m *Message) Array() ([]*Message, bool) {
	if m.Type == TypeArray {
		return m.Value.([]*Message), true
	}
	return nil, false
}

// ArrayLen returns the length of the array
func (m *Message) ArrayLen() (int, bool) {
	if m.Type == TypeArray {
		return len(m.Value.([]*Message)), true
	}
	return 0, false
}

// ErrString returns the error message string
func (m *Message) ErrString() (string, bool) {
	if m.Type == TypeError {
		return m.Value.(string), true
	}
	return "", false
}

// Marshal implements the serialization to RESP format
func (m *Message) Marshal() []byte {
	var buf bytes.Buffer

	switch m.Type {
	case TypeSimpleString:
		buf.WriteByte(byte(TypeSimpleString))
		buf.WriteString(m.Value.(string))
		buf.WriteString("\r\n")

	case TypeError:
		buf.WriteByte(byte(TypeError))
		buf.WriteString(m.Value.(string))
		buf.WriteString("\r\n")

	case TypeInteger:
		buf.WriteByte(byte(TypeInteger))
		buf.WriteString(strconv.FormatInt(m.Value.(int64), 10))
		buf.WriteString("\r\n")

	case TypeBulkString:
		buf.WriteByte(byte(TypeBulkString))
		if m.Value == nil {
			buf.WriteString("-1\r\n")
		} else {
			data := m.Value.([]byte)
			buf.WriteString(strconv.Itoa(len(data)))
			buf.WriteString("\r\n")
			buf.Write(data)
			buf.WriteString("\r\n")
		}

	case TypeArray:
		items := m.Value.([]*Message)
		buf.WriteByte(byte(TypeArray))
		buf.WriteString(strconv.Itoa(len(items)))
		buf.WriteString("\r\n")
		for _, item := range items {
			buf.Write(item.Marshal())
		}

	default:
		// Unknown type
		buf.WriteString("-ERR unknown RESP type\r\n")
	}

	return buf.Bytes()
}

// MarshalArray marshals multiple messages into a single RESP array
func MarshalArray(msgs ...*Message) []byte {
	return NewArray(msgs).Marshal()
}

// MarshalBulkString creates a bulk string RESP message
func MarshalBulkString(s string) []byte {
	if s == "" {
		return []byte("$0\r\n\r\n")
	}
	buf := make([]byte, 0, len(s)+16)
	buf = append(buf, '$')
	buf = append(buf, strconv.Itoa(len(s))...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// MarshalBulkStringBytes creates a bulk string RESP message from bytes
func MarshalBulkStringBytes(b []byte) []byte {
	if b == nil {
		return []byte("$-1\r\n")
	}
	if len(b) == 0 {
		return []byte("$0\r\n\r\n")
	}
	buf := make([]byte, 0, len(b)+16)
	buf = append(buf, '$')
	buf = append(buf, strconv.Itoa(len(b))...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, b...)
	buf = append(buf, '\r', '\n')
	return buf
}

// MarshalSimpleString creates a simple string RESP message
func MarshalSimpleString(s string) []byte {
	buf := make([]byte, 0, len(s)+5)
	buf = append(buf, '+')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// MarshalError creates an error RESP message
func MarshalError(err error) []byte {
	s := err.Error()
	buf := make([]byte, 0, len(s)+5)
	buf = append(buf, '-')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// MarshalInteger creates an integer RESP message
func MarshalInteger(i int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

// MarshalNil creates a nil bulk string RESP message
func MarshalNil() []byte {
	return []byte("$-1\r\n")
}

// MarshalOK returns the OK simple string
func MarshalOK() []byte {
	return []byte("+OK\r\n")
}

// MarshalPong returns the PONG simple string
func MarshalPong() []byte {
	return []byte("+PONG\r\n")
}

// MarshalQueued returns the QUEUED simple string for MULTI/EXEC
func MarshalQueued() []byte {
	return []byte("+QUEUED\r\n")
}

// MarshalZero returns a zero integer
func MarshalZero() []byte {
	return []byte(":0\r\n")
}

// MarshalOne returns a one integer
func MarshalOne() []byte {
	return []byte(":1\r\n")
}

// IsOK returns true if the message is an OK response
func (m *Message) IsOK() bool {
	return m.Type == TypeSimpleString && m.Value.(string) == "OK"
}

// IsPong returns true if the message is a PONG response
func (m *Message) IsPong() bool {
	return m.Type == TypeSimpleString && m.Value.(string) == "PONG"
}

// IsQueued returns true if the message is a QUEUED response
func (m *Message) IsQueued() bool {
	return m.Type == TypeSimpleString && m.Value.(string) == "QUEUED"
}
