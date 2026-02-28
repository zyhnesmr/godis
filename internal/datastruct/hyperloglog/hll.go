// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hyperloglog

import (
	"math"
)

const (
	// precision is the number of bits used for the index
	// Using 10 bits = 1024 registers for memory efficiency
	precision = 10
	// numRegisters is the number of registers (2^precision)
	numRegisters = 1 << precision
	// maxOffset is the maximum offset we can store in a register (6 bits)
	maxOffset = 63
)

// HyperLogLog represents a HyperLogLog cardinality estimator
type HyperLogLog struct {
	registers []uint8
}

// NewHyperLogLog creates a new HyperLogLog structure
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		registers: make([]uint8, numRegisters),
	}
}

// NewHyperLogLogFromBytes creates a HyperLogLog from a byte slice
// Format: each byte is a register value (simpler format)
func NewHyperLogLogFromBytes(data []byte) *HyperLogLog {
	hll := &HyperLogLog{
		registers: make([]uint8, numRegisters),
	}

	if len(data) == 0 {
		return hll
	}

	// Simple format: bytes directly represent register values
	// If data is shorter than numRegisters, pad with zeros
	// If data is longer, truncate
	copyLen := len(data)
	if copyLen > numRegisters {
		copyLen = numRegisters
	}
	copy(hll.registers, data[:copyLen])

	return hll
}

// Add adds an element to the HyperLogLog
func (hll *HyperLogLog) Add(item string) bool {
	hash := hash64(item)

	// Get the index from the first 'precision' bits
	index := hash >> (64 - precision)

	// Count the number of leading zeros in the remaining bits
	remaining := hash << precision
	offset := countLeadingZeros(remaining) + 1

	if offset > maxOffset {
		offset = maxOffset
	}

	// Update the register if the new offset is larger
	idx := int(index)
	if uint8(offset) > hll.registers[idx] {
		hll.registers[idx] = uint8(offset)
		return true
	}

	return false
}

// Count estimates the cardinality of the set
func (hll *HyperLogLog) Count() int64 {
	// Count zeros in registers
	zeroCount := 0
	sum := 0.0

	for _, r := range hll.registers {
		sum += float64(r)
		if r == 0 {
			zeroCount++
		}
	}

	// Raw estimate
	m := float64(numRegisters)

	// Alpha is a constant for bias correction
	// For different m values:
	// m = 16: alpha = 0.673
	// m = 32: alpha = 0.697
	// m = 64: alpha = 0.709
	// m >= 128: alpha = 0.7213/(1 + 1.079/m)
	var alpha float64
	if m == 16 {
		alpha = 0.673
	} else if m == 32 {
		alpha = 0.697
	} else if m == 64 {
		alpha = 0.709
	} else {
		alpha = 0.7213 / (1.0 + 1.079/m)
	}

	estimation := alpha * m * m / (m - sum)

	// Apply correction for small cardinalities
	if estimation < 2.5*m {
		if zeroCount > 0 {
			estimation = m * math.Log(m/float64(zeroCount))
		}
	}

	// Apply correction for large cardinalities
	threshold := (1.0/30.0) * float64(numRegisters) * float64(numRegisters)
	if estimation > threshold {
		ratio := estimation / (float64(numRegisters) * float64(numRegisters))
		if ratio < 1.0 {
			estimation = -m * math.Log(1.0-ratio)
		}
	}

	if estimation < 0 || math.IsNaN(estimation) || math.IsInf(estimation, 0) {
		estimation = 0
	}

	return int64(estimation)
}

// Merge merges another HyperLogLog into this one
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	for i := 0; i < numRegisters; i++ {
		if other.registers[i] > hll.registers[i] {
			hll.registers[i] = other.registers[i]
		}
	}
}

// Bytes returns the serialized representation of the HyperLogLog
// Format: each byte is a register value
func (hll *HyperLogLog) Bytes() []byte {
	// Simple format: just copy the registers
	result := make([]byte, numRegisters)
	copy(result, hll.registers)
	return result
}

// hash64 computes a 64-bit hash using MurmurHash3
func hash64(item string) uint64 {
	return murmur64([]byte(item))
}

// countLeadingZeros counts the number of leading zeros in a 64-bit integer
func countLeadingZeros(x uint64) int {
	if x == 0 {
		return 64
	}

	count := 0
	for (x & 0x8000000000000000) == 0 {
		count++
		x <<= 1
	}
	return count
}

// IsEmpty returns true if the HyperLogLog is empty (all registers are 0)
func (hll *HyperLogLog) IsEmpty() bool {
	for _, r := range hll.registers {
		if r > 0 {
			return false
		}
	}
	return true
}

// Clone creates a copy of the HyperLogLog
func (hll *HyperLogLog) Clone() *HyperLogLog {
	clone := &HyperLogLog{
		registers: make([]uint8, numRegisters),
	}
	copy(clone.registers, hll.registers)
	return clone
}

// murmur64 is a 64-bit MurmurHash2 implementation for better hash distribution
func murmur64(data []byte) uint64 {
	const (
		m = uint64(0xc6a4a7935bd1e995)
		r = 47
	)

	var h uint64 = ^uint64(0) // seed

	length := len(data)
	nblocks := length / 8

	for i := 0; i < nblocks; i++ {
		k := uint64(data[i*8]) | uint64(data[i*8+1])<<8 |
			uint64(data[i*8+2])<<16 | uint64(data[i*8+3])<<24 |
			uint64(data[i*8+4])<<32 | uint64(data[i*8+5])<<40 |
			uint64(data[i*8+6])<<48 | uint64(data[i*8+7])<<56

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	tail := data[nblocks*8:]

	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}
