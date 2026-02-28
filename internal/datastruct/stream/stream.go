// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stream

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamID represents a Redis Stream ID
// Format: <millisecondsTimestamp>-<sequenceNumber>
type StreamID struct {
	Timestamp int64 // Milliseconds since epoch
	Sequence  int64 // Sequence number
}

// ParseStreamID parses a stream ID from string format
func ParseStreamID(s string) (StreamID, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return StreamID{}, fmt.Errorf("invalid stream ID format")
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid timestamp: %w", err)
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid sequence: %w", err)
	}

	return StreamID{
		Timestamp: ts,
		Sequence:  seq,
	}, nil
}

// String returns the string representation of the stream ID
func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Timestamp, id.Sequence)
}

// IsZero returns true if the ID is the zero ID (0-0)
func (id StreamID) IsZero() bool {
	return id.Timestamp == 0 && id.Sequence == 0
}

// Compare compares two stream IDs
// Returns -1 if id < other, 0 if id == other, 1 if id > other
func (id StreamID) Compare(other StreamID) int {
	if id.Timestamp < other.Timestamp {
		return -1
	}
	if id.Timestamp > other.Timestamp {
		return 1
	}
	if id.Sequence < other.Sequence {
		return -1
	}
	if id.Sequence > other.Sequence {
		return 1
	}
	return 0
}

// AutoID creates a new stream ID with current timestamp and auto-increment sequence
func AutoID() StreamID {
	return StreamID{
		Timestamp: time.Now().UnixMilli(),
		Sequence:  0,
	}
}

// NewStreamID creates a stream ID from timestamp and sequence
func NewStreamID(timestamp int64, sequence int64) StreamID {
	return StreamID{
		Timestamp: timestamp,
		Sequence:  sequence,
	}
}

// StreamEntry represents a single message in a stream
type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
	mu     sync.RWMutex
}

// NewStreamEntry creates a new stream entry
func NewStreamEntry(id StreamID, fields map[string]string) *StreamEntry {
	return &StreamEntry{
		ID:     id,
		Fields: fields,
	}
}

// GetField returns a field value from the entry
func (e *StreamEntry) GetField(field string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	val, ok := e.Fields[field]
	return val, ok
}

// GetFields returns all fields
func (e *StreamEntry) GetFields() map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make(map[string]string, len(e.Fields))
	for k, v := range e.Fields {
		result[k] = v
	}
	return result
}

// FieldCount returns the number of fields
func (e *StreamEntry) FieldCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.Fields)
}

// Stream represents a Redis Stream data structure
type Stream struct {
	mu        sync.RWMutex
	entries   []*StreamEntry // Maintained in order by ID
	lastID    StreamID       // ID of the last entry
	length    int64          // Number of entries
	radixTree *RadixTree     // Index for fast lookup by ID prefix
	cgroups   *ConsumerGroupManager
}

// NewStream creates a new stream
func NewStream() *Stream {
	return &Stream{
		entries:   make([]*StreamEntry, 0, 64),
		lastID:    StreamID{},
		radixTree: NewRadixTree(),
		cgroups:   NewConsumerGroupManager(),
	}
}

// Add adds a new entry to the stream
// Returns the ID assigned to the entry
func (s *Stream) Add(fields map[string]string) StreamID {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate new ID
	var newID StreamID
	if s.length > 0 {
		// Try to use same timestamp as last entry
		lastEntry := s.entries[s.length-1]
		if lastEntry.ID.Timestamp == s.lastID.Timestamp {
			// Same millisecond, increment sequence
			newID = StreamID{
				Timestamp: s.lastID.Timestamp,
				Sequence:  s.lastID.Sequence + 1,
			}
		} else {
			// New timestamp
			newID = AutoID()
		}
	} else {
		// First entry
		newID = AutoID()
	}

	entry := NewStreamEntry(newID, fields)
	s.entries = append(s.entries, entry)
	s.lastID = newID
	s.length++

	// Add to radix tree for indexing
	s.radixTree.Add(newID, entry)

	return newID
}

// AddWithID adds an entry with a specific ID
// Returns an error if the ID is invalid or already exists
func (s *Stream) AddWithID(id StreamID, fields map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if ID already exists
	if s.radixTree.Find(id) != nil {
		return fmt.Errorf("ID already exists")
	}

	// ID must be greater than last ID
	if !s.lastID.IsZero() && id.Compare(s.lastID) <= 0 {
		return fmt.Errorf("ID must be greater than last ID")
	}

	entry := NewStreamEntry(id, fields)
	s.entries = append(s.entries, entry)
	s.lastID = id
	s.length++

	s.radixTree.Add(id, entry)

	return nil
}

// Range returns entries in the range [start, end]
// If start is "-", it means from the beginning
// If end is "+", it means to the end
func (s *Stream) Range(start, end string, count int64) []*StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.length == 0 {
		return nil
	}

	// Find start index
	startIdx := int64(0)
	endIdx := s.length - 1

	if start != "-" {
		startID, err := ParseStreamID(start)
		if err == nil {
			idx := s.findEntryIndex(startID)
			if idx >= 0 {
				startIdx = idx
			}
		}
	}

	if end != "+" {
		endID, err := ParseStreamID(end)
		if err == nil {
			idx := s.findEntryIndex(endID)
			if idx >= 0 {
				endIdx = idx
			}
		}
	}

	// Apply count limit
	if count > 0 && (endIdx-startIdx+1) > count {
		endIdx = startIdx + count - 1
	}

	if startIdx < 0 || startIdx >= s.length || endIdx < 0 || endIdx >= s.length || startIdx > endIdx {
		return nil
	}

	return s.entries[startIdx : endIdx+1]
}

// RevRange returns entries in reverse order
func (s *Stream) RevRange(start, end string, count int64) []*StreamEntry {
	entries := s.Range(start, end, count)
	if entries == nil {
		return nil
	}

	// Reverse the slice
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	return entries
}

// Length returns the number of entries in the stream
func (s *Stream) Length() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// FindByID finds an entry by ID
func (s *Stream) FindByID(id StreamID) *StreamEntry {
	return s.radixTree.Find(id)
}

// TrimByID trims the stream to IDs in the range [minID, maxID]
// Returns the number of entries removed
func (s *Stream) TrimByID(minID, maxID StreamID) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.length == 0 {
		return 0
	}

	// Find start and end indices
	startIdx := s.findEntryIndex(minID)
	if startIdx < 0 {
		startIdx = 0
	}

	endIdx := s.findEntryIndex(maxID)
	if endIdx < 0 {
		// maxID is greater than all entries, keep all
		endIdx = s.length - 1
	}

	if startIdx > endIdx || startIdx < 0 || endIdx >= s.length {
		// No entries in range
		if startIdx > 0 && startIdx < s.length {
			// Remove all entries before startIdx
			removed := startIdx
			s.entries = s.entries[startIdx:]
			s.length -= int64(removed)
			s.rebuildRadixTree()
			return int64(removed)
		}
		return 0
	}

	removed := startIdx

	// Keep entries from startIdx to endIdx
	newEntries := make([]*StreamEntry, 0, endIdx-startIdx+1)
	copy(newEntries, s.entries[startIdx:endIdx+1])
	s.entries = newEntries
	s.length = int64(len(newEntries))

	s.rebuildRadixTree()

	// Update lastID
	if s.length > 0 {
		s.lastID = s.entries[s.length-1].ID
	} else {
		s.lastID = StreamID{}
	}

	return int64(removed)
}

// DeleteByID deletes entries by their IDs
// Returns the number of entries deleted
func (s *Stream) DeleteByID(ids []StreamID) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.length == 0 {
		return 0
	}

	// Collect IDs to delete
	toDelete := make(map[StreamID]bool)
	for _, id := range ids {
		if s.radixTree.Find(id) != nil {
			toDelete[id] = true
		}
	}

	if len(toDelete) == 0 {
		return 0
	}

	// Filter out deleted entries
	newEntries := make([]*StreamEntry, 0, s.length)
	removed := int64(0)

	for _, entry := range s.entries {
		if toDelete[entry.ID] {
			removed++
		} else {
			newEntries = append(newEntries, entry)
		}
	}

	s.entries = newEntries
	s.length = int64(len(newEntries))

	if s.length > 0 {
		s.lastID = s.entries[s.length-1].ID
	} else {
		s.lastID = StreamID{}
	}

	s.rebuildRadixTree()

	return removed
}

// DeleteByID deletes entries by their IDs (used by XTRIM)
// Returns the number of entries deleted
func (s *Stream) XTrim(minID, maxID StreamID) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.length == 0 {
		return 0
	}

	// Find start and end indices
	startIdx := s.findEntryIndex(minID)
	if startIdx < 0 {
		startIdx = 0
	}

	endIdx := s.findEntryIndex(maxID)
	if endIdx < 0 {
		// maxID is greater than all entries, keep all
		endIdx = s.length - 1
	}

	if startIdx > endIdx || startIdx < 0 || endIdx >= s.length {
		return 0
	}

	removed := startIdx

	// Keep entries from startIdx to endIdx
	newEntries := make([]*StreamEntry, 0, endIdx-startIdx+1)
	copy(newEntries, s.entries[startIdx:endIdx+1])
	s.entries = newEntries
	s.length = int64(len(newEntries))

	s.rebuildRadixTree()

	// Update lastID
	if s.length > 0 {
		s.lastID = s.entries[s.length-1].ID
	} else {
		s.lastID = StreamID{}
	}

	return int64(removed)
}

// GetLastID returns the last ID in the stream
func (s *Stream) GetLastID() StreamID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastID
}

// GetConsumerGroupManager returns the consumer group manager
func (s *Stream) GetConsumerGroupManager() *ConsumerGroupManager {
	return s.cgroups
}

// findEntryIndex finds the index of an entry by ID using binary search
func (s *Stream) findEntryIndex(id StreamID) int64 {
	low := int64(0)
	high := s.length - 1

	for low <= high {
		mid := (low + high) / 2
		cmp := s.entries[mid].ID.Compare(id)

		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return -1
}

// rebuildRadixTree rebuilds the radix tree index from entries
func (s *Stream) rebuildRadixTree() {
	s.radixTree = NewRadixTree()
	for _, entry := range s.entries {
		s.radixTree.Add(entry.ID, entry)
	}
}

// GetEntries returns all entries (for testing/debugging)
func (s *Stream) GetEntries() []*StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*StreamEntry, len(s.entries))
	copy(result, s.entries)
	return result
}

// Clear removes all entries from the stream
func (s *Stream) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = s.entries[:0]
	s.length = 0
	s.lastID = StreamID{}
	s.radixTree = NewRadixTree()
}

// Size returns the approximate memory size of the stream in bytes
func (s *Stream) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var size int64

	// Entries
	size += int64(len(s.entries)) * 64 // Approximate per entry

	// Radix tree
	size += s.radixTree.Size()

	// Consumer groups
	if s.cgroups != nil {
		size += s.cgroups.Size()
	}

	return size
}
