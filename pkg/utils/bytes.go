// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"bytes"
)

// Equal returns true if two byte slices are equal
func Equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Contains returns true if b is contained in a
func Contains(a, b []byte) bool {
	return bytes.Contains(a, b)
}

// Index returns the index of the first occurrence of sep in s
func Index(s, sep []byte) int {
	return bytes.Index(s, sep)
}

// HasPrefix returns true if s starts with prefix
func HasPrefix(s, prefix []byte) bool {
	return bytes.HasPrefix(s, prefix)
}

// HasSuffix returns true if s ends with suffix
func HasSuffix(s, suffix []byte) bool {
	return bytes.HasSuffix(s, suffix)
}

// ToUpper converts a byte slice to uppercase
func ToUpper(s []byte) []byte {
	return bytes.ToUpper(s)
}

// ToLower converts a byte slice to lowercase
func ToLower(s []byte) []byte {
	return bytes.ToLower(s)
}

// Trim removes leading and trailing characters
func Trim(s []byte, cutset string) []byte {
	return bytes.Trim(s, cutset)
}

// TrimSpace removes leading and trailing whitespace
func TrimSpace(s []byte) []byte {
	return bytes.TrimSpace(s)
}

// Split splits a byte slice by sep
func Split(s, sep []byte) [][]byte {
	return bytes.Split(s, sep)
}

// Join joins byte slices with sep
func Join(s [][]byte, sep []byte) []byte {
	return bytes.Join(s, sep)
}

// Repeat repeats a byte slice count times
func Repeat(b []byte, count int) []byte {
	return bytes.Repeat(b, count)
}

// Replace replaces occurrences of old with new
func Replace(s, old, new []byte, n int) []byte {
	return bytes.Replace(s, old, new, n)
}
