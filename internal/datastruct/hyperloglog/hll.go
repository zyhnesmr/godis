// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hyperloglog

import (
	"encoding/binary"
	"hash/fnv"
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

	// Calculate alpha based on precision
	alpha := 0.673 * (1.0 + 0.715/float64(numRegisters))

	// Raw estimate
	m := float64(numRegisters)
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

// hash64 computes a 64-bit hash using FNV-1a
func hash64(item string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(item))
	sum := h.Sum(nil)
	// Ensure we have at least 8 bytes
	if len(sum) < 8 {
		padded := make([]byte, 8)
		copy(padded, sum)
		sum = padded
	}
	return binary.BigEndian.Uint64(sum)
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
