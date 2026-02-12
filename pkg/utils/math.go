// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"math"
)

// Max returns the maximum of two integers
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Min returns the minimum of two integers
func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint returns the maximum of two unsigned integers
func MaxUint(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// MinUint returns the minimum of two unsigned integers
func MinUint(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// Abs returns the absolute value of an integer
func Abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// Pow returns base^exp
func Pow(base, exp int64) int64 {
	return int64(math.Pow(float64(base), float64(exp)))
}

// Sqrt returns the square root of n
func Sqrt(n int64) int64 {
	return int64(math.Sqrt(float64(n)))
}

// Ceil returns the ceiling of a float
func Ceil(f float64) int64 {
	return int64(math.Ceil(f))
}

// Floor returns the floor of a float
func Floor(f float64) int64 {
	return int64(math.Floor(f))
}

// Round returns the rounded integer
func Round(f float64) int64 {
	return int64(math.Round(f))
}

// Clamp clamps a value between min and max
func Clamp(n, min, max int64) int64 {
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

// Sum returns the sum of values
func Sum(values ...int64) int64 {
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum
}

// Average returns the average of values
func Average(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}
	return Sum(values...) / int64(len(values))
}

// GCD returns the greatest common divisor
func GCD(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// LCM returns the least common multiple
func LCM(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	return Abs(a*b) / GCD(a, b)
}
