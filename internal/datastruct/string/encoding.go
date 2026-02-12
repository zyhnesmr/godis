// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package str

import (
	"strconv"
)

// Encoding represents the string encoding type
type Encoding int

const (
	// EncodingRaw is for strings longer than 44 bytes
	EncodingRaw Encoding = iota

	// EncodingInt is for integers up to 64-bit
	EncodingInt

	// EncodingEmbstr is for strings up to 44 bytes
	EncodingEmbstr
)

const (
	// EmbstrMaxLen is the maximum length for embstr encoding
	EmbstrMaxLen = 44

	// IntMaxLen is the maximum digits for integer encoding
	IntMaxLen = 20
)

// SelectEncoding selects the optimal encoding for a string
func SelectEncoding(s string) Encoding {
	// Try to encode as integer
	if len(s) <= IntMaxLen {
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return EncodingInt
		}
	}

	// Use embstr for short strings
	if len(s) <= EmbstrMaxLen {
		return EncodingEmbstr
	}

	// Use raw for longer strings
	return EncodingRaw
}

// IsIntEncoded returns true if the string is integer encoded
func IsIntEncoded(s string) bool {
	if len(s) > IntMaxLen {
		return false
	}
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

// ParseInt parses a string to int64
func ParseInt(s string) (int64, bool) {
	val, err := strconv.ParseInt(s, 10, 64)
	return val, err == nil
}

// ParseFloat parses a string to float64
func ParseFloat(s string) (float64, bool) {
	val, err := strconv.ParseFloat(s, 64)
	return val, err == nil
}

// FormatInt formats an int64 to string
func FormatInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

// FormatFloat formats a float64 to string
func FormatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// TryIncr tries to increment a string value
func TryIncr(s string, delta int64) (string, error) {
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "", err
	}
	newVal := val + delta
	return strconv.FormatInt(newVal, 10), nil
}

// TryIncrFloat tries to increment a string value by float
func TryIncrFloat(s string, delta float64) (string, error) {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", err
	}
	newVal := val + delta
	return strconv.FormatFloat(newVal, 'f', -1, 64), nil
}
