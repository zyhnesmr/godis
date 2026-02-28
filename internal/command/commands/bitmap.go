// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	strpkg "github.com/zyhnesmr/godis/internal/datastruct/string"
)

// RegisterBitmapCommands registers all bitmap commands
func RegisterBitmapCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "SETBIT",
		Handler:    setbitCmd,
		Arity:      4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "GETBIT",
		Handler:    getbitCmd,
		Arity:      3,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "BITCOUNT",
		Handler:    bitcountCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly, command.FlagFast},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "BITPOS",
		Handler:    bitposCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "BITOP",
		Handler:    bitopCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   2,
		LastKey:    -1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "BITFIELD",
		Handler:    bitfieldCmd,
		Arity:      -3,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})

	disp.Register(&command.Command{
		Name:       "BITFIELD_RO",
		Handler:    bitfieldRoCmd,
		Arity:      -3,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatString, command.CatGeneric},
	})
}

// SETBIT key offset value
func setbitCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	offset, err := strconv.Atoi(ctx.Args[1])
	if err != nil || offset < 0 {
		return nil, errors.New("bit offset is not an integer or out of range")
	}

	value, err := strconv.Atoi(ctx.Args[2])
	if err != nil || (value != 0 && value != 1) {
		return nil, errors.New("bit is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Create new string with null bytes
		str := strpkg.NewString("")
		oldValue := str.SetBit(offset, value)
		ctx.DB.Set(key, database.NewStringObject(str.String()))
		return command.NewIntegerReply(int64(oldValue)), nil
	}

	// Get current string value
	currentStr := obj.String()
	str := strpkg.NewString(currentStr)
	oldValue := str.SetBit(offset, value)

	ctx.DB.Set(key, database.NewStringObject(str.String()))
	return command.NewIntegerReply(int64(oldValue)), nil
}

// GETBIT key offset
func getbitCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) != 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	offset, err := strconv.Atoi(ctx.Args[1])
	if err != nil || offset < 0 {
		return nil, errors.New("bit offset is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	currentStr := obj.String()
	str := strpkg.NewString(currentStr)
	bitValue := str.GetBit(offset)

	return command.NewIntegerReply(int64(bitValue)), nil
}

// BITCOUNT key [start end]
func bitcountCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewIntegerReply(0), nil
	}

	currentStr := obj.String()
	str := strpkg.NewString(currentStr)

	// Default: count entire string
	start := 0
	end := len(currentStr) - 1

	// Parse start and end if provided
	if len(ctx.Args) >= 2 {
		s, err := strconv.Atoi(ctx.Args[1])
		if err != nil {
			return nil, errors.New("start is not an integer or out of range")
		}
		start = s
	}

	if len(ctx.Args) >= 3 {
		e, err := strconv.Atoi(ctx.Args[2])
		if err != nil {
			return nil, errors.New("end is not an integer or out of range")
		}
		end = e
	}

	count := str.BitCount(start, end)
	return command.NewIntegerReply(int64(count)), nil
}

// BITPOS key bit [start end]
func bitposCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	bit, err := strconv.Atoi(ctx.Args[1])
	if err != nil || (bit != 0 && bit != 1) {
		return nil, errors.New("bit is not an integer or out of range")
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Empty string: bit 0 is at position 0, bit 1 is at -1
		if bit == 0 {
			return command.NewIntegerReply(0), nil
		}
		return command.NewIntegerReply(-1), nil
	}

	currentStr := obj.String()
	if len(currentStr) == 0 {
		if bit == 0 {
			return command.NewIntegerReply(0), nil
		}
		return command.NewIntegerReply(-1), nil
	}

	// Parse start and end
	start := 0
	end := len(currentStr) - 1

	if len(ctx.Args) >= 3 {
		s, err := strconv.Atoi(ctx.Args[2])
		if err != nil {
			return nil, errors.New("start is not an integer or out of range")
		}
		start = s
	}

	if len(ctx.Args) >= 4 {
		e, err := strconv.Atoi(ctx.Args[3])
		if err != nil {
			return nil, errors.New("end is not an integer or out of range")
		}
		end = e
	}

	// Normalize indices
	runes := []rune(currentStr)
	length := len(runes)

	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	if start >= length {
		start = length
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return command.NewIntegerReply(-1), nil
	}

	// Search for the bit
	for byteIdx := start; byteIdx <= end; byteIdx++ {
		b := currentStr[byteIdx]
		for bitIdx := 0; bitIdx < 8; bitIdx++ {
			currentBit := (b >> (7 - bitIdx)) & 1
			if currentBit == byte(bit) {
				return command.NewIntegerReply(int64(byteIdx*8 + bitIdx)), nil
			}
		}
	}

	// Bit not found in range, check if we should return the next position
	if bit == 0 && end < length-1 {
		// Return first 0 bit after the range
		for byteIdx := end + 1; byteIdx < length; byteIdx++ {
			b := currentStr[byteIdx]
			for bitIdx := 0; bitIdx < 8; bitIdx++ {
				currentBit := (b >> (7 - bitIdx)) & 1
				if currentBit == 0 {
					return command.NewIntegerReply(int64(byteIdx*8 + bitIdx)), nil
				}
			}
		}
	}

	return command.NewIntegerReply(-1), nil
}

// BITOP operation destkey key [key ...]
func bitopCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	operation := strings.ToUpper(ctx.Args[0])
	destKey := ctx.Args[1]
	srcKeys := ctx.Args[2:]

	if len(srcKeys) == 0 {
		return nil, errors.New("BITOP requires at least one source key")
	}

	// Validate operation
	switch operation {
	case "AND", "OR", "XOR", "NOT":
	default:
		return nil, errors.New("unknown BITOP operation")
	}

	// NOT requires exactly one source key
	if operation == "NOT" && len(srcKeys) != 1 {
		return nil, errors.New("BITOP NOT requires exactly one source key")
	}

	// Collect source strings as byte slices
	var srcBytes [][]byte
	maxLen := 0
	for _, key := range srcKeys {
		if obj, ok := ctx.DB.Get(key); ok {
			b := []byte(obj.String())
			srcBytes = append(srcBytes, b)
			if len(b) > maxLen {
				maxLen = len(b)
			}
		} else {
			srcBytes = append(srcBytes, []byte{})
		}
	}

	// Handle empty case
	if maxLen == 0 {
		ctx.DB.Set(destKey, database.NewStringObject(""))
		return command.NewIntegerReply(0), nil
	}

	// Perform the operation
	var result []byte

	switch operation {
	case "AND":
		result = make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			result[i] = 0xFF
			for _, src := range srcBytes {
				if i < len(src) {
					result[i] &= src[i]
				} else {
					result[i] &= 0
				}
			}
		}
	case "OR":
		result = make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			result[i] = 0
			for _, src := range srcBytes {
				if i < len(src) {
					result[i] |= src[i]
				}
			}
		}
	case "XOR":
		result = make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			result[i] = 0
			for _, src := range srcBytes {
				if i < len(src) {
					result[i] ^= src[i]
				}
			}
		}
	case "NOT":
		src := srcBytes[0]
		result = make([]byte, len(src))
		for i := 0; i < len(src); i++ {
			result[i] = ^src[i]
		}
		maxLen = len(src)
	}

	// Store result
	ctx.DB.Set(destKey, database.NewStringObject(string(result)))

	return command.NewIntegerReply(int64(maxLen)), nil
}

// BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment]
func bitfieldCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	args := ctx.Args[1:]

	if len(args) == 0 {
		return nil, errors.New("wrong number of arguments")
	}

	// Get or create the string
	var currentStr string
	if obj, ok := ctx.DB.Get(key); ok {
		currentStr = obj.String()
	}

	results := make([]interface{}, 0)
	overflow := "FAIL"

	for i := 0; i < len(args); i++ {
		subcommand := strings.ToUpper(args[i])

		switch subcommand {
		case "GET":
			if i+2 >= len(args) {
				return nil, errors.New("GET requires encoding and offset")
			}
			encoding := args[i+1]
			offset, err := parseBitfieldOffset(args[i+2])
			if err != nil {
				return nil, err
			}
			i += 2

			value, _ := getBitfield(currentStr, encoding, offset)
			results = append(results, value)

		case "SET":
			if i+3 >= len(args) {
				return nil, errors.New("SET requires encoding, offset, and value")
			}
			encoding := args[i+1]
			offset, err := parseBitfieldOffset(args[i+2])
			if err != nil {
				return nil, err
			}
			value, err := strconv.ParseInt(args[i+3], 10, 64)
			if err != nil {
				return nil, errors.New("value is not an integer")
			}
			i += 3

			oldValue, newStr, err := setBitfield(currentStr, encoding, offset, value)
			if err != nil {
				return nil, err
			}
			currentStr = newStr
			results = append(results, oldValue)

		case "INCRBY":
			if i+3 >= len(args) {
				return nil, errors.New("INCRBY requires encoding, offset, and increment")
			}
			encoding := args[i+1]
			offset, err := parseBitfieldOffset(args[i+2])
			if err != nil {
				return nil, err
			}
			increment, err := strconv.ParseInt(args[i+3], 10, 64)
			if err != nil {
				return nil, errors.New("increment is not an integer")
			}
			i += 3

			newValue, newStr, err := incrbyBitfield(currentStr, encoding, offset, increment, overflow)
			if err != nil {
				if err.Error() == "overflow" {
					results = append(results, nil)
				} else {
					return nil, err
				}
			} else {
				currentStr = newStr
				results = append(results, newValue)
			}

		case "OVERFLOW":
			if i+1 >= len(args) {
				return nil, errors.New("OVERFLOW requires a type")
			}
			overflow = strings.ToUpper(args[i+1])
			if overflow != "WRAP" && overflow != "SAT" && overflow != "FAIL" {
				return nil, errors.New("invalid overflow type")
			}
			i += 1

		default:
			return nil, errors.New("unknown subcommand")
		}
	}

	// Store the modified string
	ctx.DB.Set(key, database.NewStringObject(currentStr))

	return command.NewArrayReplyFromAny(results), nil
}

// BITFIELD_RO key [GET encoding offset] ...
func bitfieldRoCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 1 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	args := ctx.Args[1:]

	if len(args) == 0 {
		return nil, errors.New("wrong number of arguments")
	}

	// Get the string
	var currentStr string
	if obj, ok := ctx.DB.Get(key); ok {
		currentStr = obj.String()
	}

	results := make([]interface{}, 0)

	for i := 0; i < len(args); i++ {
		subcommand := strings.ToUpper(args[i])

		if subcommand == "GET" {
			if i+2 >= len(args) {
				return nil, errors.New("GET requires encoding and offset")
			}
			encoding := args[i+1]
			offset, err := parseBitfieldOffset(args[i+2])
			if err != nil {
				return nil, err
			}
			i += 2

			value, _ := getBitfield(currentStr, encoding, offset)
			results = append(results, value)
		} else {
			return nil, errors.New("unknown or unsupported subcommand")
		}
	}

	return command.NewArrayReplyFromAny(results), nil
}

// parseBitfieldOffset parses a bitfield offset which can be like "#1" or just a number
func parseBitfieldOffset(s string) (int, error) {
	if strings.HasPrefix(s, "#") {
		offset, err := strconv.Atoi(s[1:])
		if err != nil {
			return 0, errors.New("offset is not an integer")
		}
		return offset * 4, nil // Multiplied by 4 bytes (32 bits)
	}
	offset, err := strconv.Atoi(s)
	if err != nil {
		return 0, errors.New("offset is not an integer")
	}
	return offset, nil
}

// parseBitfieldEncoding parses bitfield encoding like "i5", "u8", etc.
func parseBitfieldEncoding(encoding string) (bool, int, error) {
	if len(encoding) < 2 {
		return false, 0, errors.New("invalid encoding")
	}

	signed := encoding[0] == 'i'
	if !signed && encoding[0] != 'u' {
		return false, 0, errors.New("invalid encoding")
	}

	bits, err := strconv.Atoi(encoding[1:])
	if err != nil || bits < 1 || bits > 64 {
		return false, 0, errors.New("invalid encoding")
	}

	return signed, bits, nil
}

// getBitfield gets a bitfield value
func getBitfield(s string, encoding string, offset int) (int64, error) {
	signed, bits, err := parseBitfieldEncoding(encoding)
	if err != nil {
		return 0, err
	}

	byteLen := (bits + 7) / 8
	bitOffset := offset % 8
	byteOffset := offset / 8

	// Get the bytes containing our field
	value := int64(0)
	for i := 0; i < byteLen; i++ {
		byteIdx := byteOffset + i
		if byteIdx >= len(s) {
			break
		}
		b := byte(s[byteIdx])
		value |= int64(b) << (8 * (byteLen - 1 - i))
	}

	// Shift to align with our bit offset
	value >>= (8 - bitOffset - bits%8) % 8
	if bits%8 != 0 {
		value >>= 8 - bits%8
	}

	// Mask to get only our bits
	mask := int64(1)<<bits - 1
	value &= mask

	// Handle signed values
	if signed && (value&(1<<(bits-1))) != 0 {
		value |= ^mask
	}

	return value, nil
}

// setBitfield sets a bitfield value
func setBitfield(s string, encoding string, offset int, newValue int64) (int64, string, error) {
	_, bits, err := parseBitfieldEncoding(encoding)
	if err != nil {
		return 0, "", err
	}

	// Ensure string is long enough
	byteLen := (bits + 7) / 8
	byteOffset := offset / 8
	requiredLen := byteOffset + byteLen
	for len(s) < requiredLen {
		s += "\x00"
	}

	// Get old value
	oldValue, _ := getBitfield(s, encoding, offset)

	// Set new value (simplified implementation)
	mask := int64(1)<<bits - 1
	newValue &= mask

	// Convert to bytes and set
	bytes := []byte(s)
	for i := 0; i < byteLen; i++ {
		byteIdx := byteOffset + i
		shift := (byteLen - 1 - i) * 8
		bytes[byteIdx] = byte(newValue >> shift)
	}

	s = string(bytes)

	return oldValue, s, nil
}

// incrbyBitfield increments a bitfield value
func incrbyBitfield(s string, encoding string, offset int, increment int64, overflow string) (int64, string, error) {
	signed, bits, err := parseBitfieldEncoding(encoding)
	if err != nil {
		return 0, "", err
	}

	// Get current value
	currentValue, _ := getBitfield(s, encoding, offset)

	newValue := currentValue + increment

	// Check for overflow
	maxValue := int64(1)<<bits - 1
	minValue := int64(0)
	if signed {
		maxValue = int64(1<<(bits-1)) - 1
		minValue = -int64(1 << (bits - 1))
	}

	// Handle overflow based on overflow type
	if newValue > maxValue || newValue < minValue {
		switch overflow {
		case "FAIL":
			return 0, "", errors.New("overflow")
		case "WRAP":
			if newValue > maxValue {
				newValue -= (maxValue - minValue + 1)
			} else if newValue < minValue {
				newValue += (maxValue - minValue + 1)
			}
		case "SAT":
			if newValue > maxValue {
				newValue = maxValue
			} else if newValue < minValue {
				newValue = minValue
			}
		}
	}

	// Set the new value
	_, newStr, err := setBitfield(s, encoding, offset, newValue)
	if err != nil {
		return 0, "", err
	}

	return newValue, newStr, nil
}
