// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resp

import (
	"fmt"
	"io"
	"strconv"
)

// Serializer writes RESP protocol messages to an output stream
type Serializer struct {
	writer io.Writer
}

// NewSerializer creates a new RESP serializer
func NewSerializer(writer io.Writer) *Serializer {
	return &Serializer{writer: writer}
}

// Write writes raw data to the output stream
func (s *Serializer) Write(data []byte) error {
	_, err := s.writer.Write(data)
	return err
}

// WriteString writes a string to the output stream
func (s *Serializer) WriteString(str string) error {
	_, err := s.writer.Write([]byte(str))
	return err
}

// WriteSimpleString writes a simple string RESP message
func (s *Serializer) WriteSimpleString(str string) error {
	_, err := s.writer.Write([]byte("+" + str + "\r\n"))
	return err
}

// WriteError writes an error RESP message
func (s *Serializer) WriteError(err error) error {
	_, writeErr := s.writer.Write([]byte("-" + err.Error() + "\r\n"))
	return writeErr
}

// WriteErrorString writes an error string as a RESP error message
func (s *Serializer) WriteErrorString(errStr string) error {
	_, err := s.writer.Write([]byte("-" + errStr + "\r\n"))
	return err
}

// WriteInteger writes an integer RESP message
func (s *Serializer) WriteInteger(i int64) error {
	_, err := s.writer.Write([]byte(":" + strconv.FormatInt(i, 10) + "\r\n"))
	return err
}

// WriteBulkString writes a bulk string RESP message
func (s *Serializer) WriteBulkString(data []byte) error {
	if data == nil {
		_, err := s.writer.Write([]byte("$-1\r\n"))
		return err
	}
	_, err := s.writer.Write([]byte("$" + strconv.Itoa(len(data)) + "\r\n"))
	if err != nil {
		return err
	}
	_, err = s.writer.Write(data)
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]byte("\r\n"))
	return err
}

// WriteBulkStringFromString writes a string as a bulk string RESP message
func (s *Serializer) WriteBulkStringFromString(str string) error {
	if str == "" {
		_, err := s.writer.Write([]byte("$0\r\n\r\n"))
		return err
	}
	_, err := s.writer.Write([]byte("$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"))
	return err
}

// WriteArray writes an array RESP message header
func (s *Serializer) WriteArray(count int) error {
	_, err := s.writer.Write([]byte("*" + strconv.Itoa(count) + "\r\n"))
	return err
}

// WriteNilArray writes a nil array RESP message
func (s *Serializer) WriteNilArray() error {
	_, err := s.writer.Write([]byte("*-1\r\n"))
	return err
}

// WriteEmptyArray writes an empty array RESP message
func (s *Serializer) WriteEmptyArray() error {
	_, err := s.writer.Write([]byte("*0\r\n"))
	return err
}

// WriteMessage writes a complete RESP message
func (s *Serializer) WriteMessage(msg *Message) error {
	_, err := s.writer.Write(msg.Marshal())
	return err
}

// Flush flushes the output stream
func (s *Serializer) Flush() error {
	if flusher, ok := s.writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

// ResponseBuilder helps build complex RESP responses
type ResponseBuilder struct {
	buf []byte
}

// NewResponseBuilder creates a new response builder
func NewResponseBuilder() *ResponseBuilder {
	return &ResponseBuilder{buf: make([]byte, 0, 64)}
}

// Bytes returns the built response as bytes
func (b *ResponseBuilder) Bytes() []byte {
	return b.buf
}

// String returns the built response as a string
func (b *ResponseBuilder) String() string {
	return string(b.buf)
}

// Reset clears the builder buffer
func (b *ResponseBuilder) Reset() {
	b.buf = b.buf[:0]
}

// WriteSimpleString writes a simple string to the buffer
func (b *ResponseBuilder) WriteSimpleString(str string) *ResponseBuilder {
	b.buf = append(b.buf, '+')
	b.buf = append(b.buf, str...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteError writes an error to the buffer
func (b *ResponseBuilder) WriteError(err error) *ResponseBuilder {
	b.buf = append(b.buf, '-')
	b.buf = append(b.buf, err.Error()...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteErrorString writes an error string to the buffer
func (b *ResponseBuilder) WriteErrorString(err string) *ResponseBuilder {
	b.buf = append(b.buf, '-')
	b.buf = append(b.buf, err...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteInteger writes an integer to the buffer
func (b *ResponseBuilder) WriteInteger(i int64) *ResponseBuilder {
	b.buf = append(b.buf, ':')
	b.buf = append(b.buf, strconv.FormatInt(i, 10)...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteBulkString writes a bulk string to the buffer
func (b *ResponseBuilder) WriteBulkString(data []byte) *ResponseBuilder {
	if data == nil {
		b.buf = append(b.buf, "$-1\r\n"...)
		return b
	}
	b.buf = append(b.buf, '$')
	b.buf = append(b.buf, strconv.Itoa(len(data))...)
	b.buf = append(b.buf, '\r', '\n')
	b.buf = append(b.buf, data...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteBulkStringFromString writes a string as a bulk string to the buffer
func (b *ResponseBuilder) WriteBulkStringFromString(str string) *ResponseBuilder {
	if str == "" {
		b.buf = append(b.buf, "$0\r\n\r\n"...)
		return b
	}
	b.buf = append(b.buf, '$')
	b.buf = append(b.buf, strconv.Itoa(len(str))...)
	b.buf = append(b.buf, '\r', '\n')
	b.buf = append(b.buf, str...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteNil writes a nil bulk string to the buffer
func (b *ResponseBuilder) WriteNil() *ResponseBuilder {
	b.buf = append(b.buf, "$-1\r\n"...)
	return b
}

// WriteArray writes an array header to the buffer
func (b *ResponseBuilder) WriteArray(count int) *ResponseBuilder {
	b.buf = append(b.buf, '*')
	b.buf = append(b.buf, strconv.Itoa(count)...)
	b.buf = append(b.buf, '\r', '\n')
	return b
}

// WriteStringArray writes an array of strings to the buffer
func (b *ResponseBuilder) WriteStringArray(strs []string) *ResponseBuilder {
	b.WriteArray(len(strs))
	for _, s := range strs {
		b.WriteBulkStringFromString(s)
	}
	return b
}

// WriteBulkStringArray writes an array of bulk strings to the buffer
func (b *ResponseBuilder) WriteBulkStringArray(data [][]byte) *ResponseBuilder {
	b.WriteArray(len(data))
	for _, d := range data {
		b.WriteBulkString(d)
	}
	return b
}

// WriteOK writes an OK response
func (b *ResponseBuilder) WriteOK() *ResponseBuilder {
	b.buf = append(b.buf, "+OK\r\n"...)
	return b
}

// WritePong writes a PONG response
func (b *ResponseBuilder) WritePong() *ResponseBuilder {
	b.buf = append(b.buf, "+PONG\r\n"...)
	return b
}

// WriteQueued writes a QUEUED response for MULTI/EXEC
func (b *ResponseBuilder) WriteQueued() *ResponseBuilder {
	b.buf = append(b.buf, "+QUEUED\r\n"...)
	return b
}

// WriteZero writes a zero integer
func (b *ResponseBuilder) WriteZero() *ResponseBuilder {
	b.buf = append(b.buf, ":0\r\n"...)
	return b
}

// WriteOne writes a one integer
func (b *ResponseBuilder) WriteOne() *ResponseBuilder {
	b.buf = append(b.buf, ":1\r\n"...)
	return b
}

// Clone creates a copy of the response builder
func (b *ResponseBuilder) Clone() *ResponseBuilder {
	clone := &ResponseBuilder{
		buf: make([]byte, len(b.buf)),
	}
	copy(clone.buf, b.buf)
	return clone
}

// Helper functions for common responses

// BuildOK creates an OK response
func BuildOK() []byte {
	return []byte("+OK\r\n")
}

// BuildPong creates a PONG response
func BuildPong() []byte {
	return []byte("+PONG\r\n")
}

// BuildQueued creates a QUEUED response
func BuildQueued() []byte {
	return []byte("+QUEUED\r\n")
}

// BuildNil creates a nil bulk string response
func BuildNil() []byte {
	return []byte("$-1\r\n")
}

// BuildZero creates a zero integer response
func BuildZero() []byte {
	return []byte(":0\r\n")
}

// BuildOne creates a one integer response
func BuildOne() []byte {
	return []byte(":1\r\n")
}

// BuildBulkString creates a bulk string response
func BuildBulkString(s string) []byte {
	if s == "" {
		return []byte("$0\r\n\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// BuildBulkStringBytes creates a bulk string response from bytes
func BuildBulkStringBytes(b []byte) []byte {
	if b == nil {
		return []byte("$-1\r\n")
	}
	if len(b) == 0 {
		return []byte("$0\r\n\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b), string(b)))
}

// BuildInteger creates an integer response
func BuildInteger(i int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

// BuildError creates an error response
func BuildError(err error) []byte {
	return []byte("-" + err.Error() + "\r\n")
}

// BuildErrorString creates an error response from a string
func BuildErrorString(err string) []byte {
	return []byte("-" + err + "\r\n")
}

// BuildArray creates an array response from messages
func BuildArray(msgs ...*Message) []byte {
	return NewArray(msgs).Marshal()
}

// BuildStringArray creates an array response from strings
func BuildStringArray(strs []string) []byte {
	builder := NewResponseBuilder()
	builder.WriteStringArray(strs)
	return builder.Bytes()
}

// BuildBulkStringArray creates an array response from byte slices
func BuildBulkStringArray(data [][]byte) []byte {
	builder := NewResponseBuilder()
	builder.WriteBulkStringArray(data)
	return builder.Bytes()
}

// BuildEmptyArray creates an empty array response
func BuildEmptyArray() []byte {
	return []byte("*0\r\n")
}

// BuildNilArray creates a nil array response
func BuildNilArray() []byte {
	return []byte("*-1\r\n")
}

// BuildSimpleString creates a simple string response
func BuildSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// WriteBytes appends raw bytes to the buffer
func (b *ResponseBuilder) WriteBytes(data []byte) *ResponseBuilder {
	b.buf = append(b.buf, data...)
	return b
}
