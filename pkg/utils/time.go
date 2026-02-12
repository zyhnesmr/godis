// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"time"
)

// NowMs returns current time in milliseconds
func NowMs() int64 {
	return time.Now().UnixMilli()
}

// NowUs returns current time in microseconds
func NowUs() int64 {
	return time.Now().UnixMicro()
}

// NowNs returns current time in nanoseconds
func NowNs() int64 {
	return time.Now().UnixNano()
}

// UnixMs converts Unix timestamp in milliseconds to time.Time
func UnixMs(ms int64) time.Time {
	return time.Unix(ms/1000, (ms%1000)*1e6)
}

// UsToDuration converts microseconds to duration
func UsToDuration(us int64) time.Duration {
	return time.Duration(us) * time.Microsecond
}

// MsToDuration converts milliseconds to duration
func MsToDuration(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// SToDuration converts seconds to duration
func SToDuration(s int64) time.Duration {
	return time.Duration(s) * time.Second
}

// DurationToMs converts duration to milliseconds
func DurationToMs(d time.Duration) int64 {
	return d.Milliseconds()
}

// DurationToUs converts duration to microseconds
func DurationToUs(d time.Duration) int64 {
	return d.Microseconds()
}

// DurationToS converts duration to seconds
func DurationToS(d time.Duration) int64 {
	return int64(d.Seconds())
}

// TimeToMs converts time.Time to milliseconds since epoch
func TimeToMs(t time.Time) int64 {
	return t.UnixMilli()
}

// TimeToUs converts time.Time to microseconds since epoch
func TimeToUs(t time.Time) int64 {
	return t.UnixMicro()
}

// FormatDuration formats a duration in a human-readable way
func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return d.String()
	}
	if d < time.Millisecond {
		return d.Truncate(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Truncate(time.Millisecond).String()
	}
	if d < time.Minute {
		return d.Truncate(time.Second).String()
	}
	if d < time.Hour {
		return d.Truncate(time.Minute).String()
	}
	return d.Truncate(time.Hour).String()
}
