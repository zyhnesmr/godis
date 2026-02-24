// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package set

import (
	"math/rand/v2"
	"sync"
)

// SetEncoding represents the encoding type of a set
type SetEncoding byte

const (
	// SetEncodingHashtable uses a Go map
	SetEncodingHashtable SetEncoding = iota
	// SetEncodingIntset uses an intset for integer-only sets (TODO)
	SetEncodingIntset
)

// Set represents a Redis set data structure
type Set struct {
	mu       sync.RWMutex
	data     map[string]struct{}
	encoding SetEncoding
}

// NewSet creates a new set
func NewSet() *Set {
	return &Set{
		data:     make(map[string]struct{}),
		encoding: SetEncodingHashtable,
	}
}

// NewSetFromSlice creates a set from a slice
func NewSetFromSlice(items []string) *Set {
	s := &Set{
		data:     make(map[string]struct{}, len(items)),
		encoding: SetEncodingHashtable,
	}
	for _, item := range items {
		s.data[item] = struct{}{}
	}
	return s
}

// Add adds a member to the set
// Returns the number of new members added
func (s *Set) Add(member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[member]; exists {
		return 0
	}
	s.data[member] = struct{}{}
	return 1
}

// AddMultiple adds multiple members to the set
// Returns the number of new members added
func (s *Set) AddMultiple(members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	for _, member := range members {
		if _, exists := s.data[member]; !exists {
			s.data[member] = struct{}{}
			added++
		}
	}
	return added
}

// Remove removes a member from the set
// Returns true if the member was removed
func (s *Set) Remove(member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[member]; exists {
		delete(s.data, member)
		return true
	}
	return false
}

// RemoveMultiple removes multiple members from the set
// Returns the number of members removed
func (s *Set) RemoveMultiple(members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, member := range members {
		if _, exists := s.data[member]; exists {
			delete(s.data, member)
			removed++
		}
	}
	return removed
}

// Contains checks if a member exists in the set
func (s *Set) Contains(member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[member]
	return exists
}

// ContainsMultiple checks if multiple members exist in the set
// Returns a slice of 1/0 for each member
func (s *Set) ContainsMultiple(members []string) []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]int, len(members))
	for i, member := range members {
		if _, exists := s.data[member]; exists {
			result[i] = 1
		} else {
			result[i] = 0
		}
	}
	return result
}

// Len returns the number of members in the set
func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

// Members returns all members of the set
func (s *Set) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]string, 0, len(s.data))
	for member := range s.data {
		members = append(members, member)
	}
	return members
}

// Pop removes and returns a random member from the set
// Returns empty string and false if set is empty
func (s *Set) Pop() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return "", false
	}

	// Get a random member
	for member := range s.data {
		delete(s.data, member)
		return member, true
	}

	return "", false
}

// PopMultiple removes and returns multiple random members
func (s *Set) PopMultiple(count int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return nil
	}

	result := []string{}
	for i := 0; i < count && len(s.data) > 0; i++ {
		for member := range s.data {
			delete(s.data, member)
			result = append(result, member)
			break
		}
	}

	return result
}

// RandomMember returns a random member without removing it
func (s *Set) RandomMember() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return "", false
	}

	for member := range s.data {
		return member, true
	}

	return "", false
}

// RandomMembers returns multiple random members without removing them
func (s *Set) RandomMembers(count int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	members := make([]string, 0, len(s.data))
	for member := range s.data {
		members = append(members, member)
	}

	// Shuffle and return first count
	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})

	if count > len(members) {
		count = len(members)
	}

	return members[:count]
}

// RandomMembersDistinct returns distinct random members
func (s *Set) RandomMembersDistinct(count int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	members := make([]string, 0, len(s.data))
	for member := range s.data {
		members = append(members, member)
	}

	if count >= len(members) {
		return members
	}

	// Fisher-Yates shuffle for random selection
	result := make([]string, count)
	indices := rand.Perm(len(members))
	for i := 0; i < count; i++ {
		result[i] = members[indices[i]]
	}

	return result
}

// Move moves a member from this set to another set
// Returns true if the member was moved
func (s *Set) MoveTo(member string, dest *Set) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	dest.mu.Lock()
	defer dest.mu.Unlock()

	if _, exists := s.data[member]; !exists {
		return false
	}

	// Check if destination already has the member
	if _, exists := dest.data[member]; exists {
		return false
	}

	delete(s.data, member)
	dest.data[member] = struct{}{}
	return true
}

// Diff returns members that are in this set but not in the other sets
func (s *Set) Diff(others []*Set) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	result := []string{}
	for member := range s.data {
		found := false
		for _, other := range others {
			if _, exists := other.data[member]; exists {
				found = true
				break
			}
		}
		if !found {
			result = append(result, member)
		}
	}

	return result
}

// Intersect returns members that are in all sets
func (s *Set) Intersect(others []*Set) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	result := []string{}
	for member := range s.data {
		inAll := true
		for _, other := range others {
			if _, exists := other.data[member]; !exists {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, member)
		}
	}

	return result
}

// Union returns members that are in any of the sets
func (s *Set) Union(others []*Set) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	seen := make(map[string]struct{})
	result := []string{}

	// Add members from this set
	for member := range s.data {
		if _, exists := seen[member]; !exists {
			seen[member] = struct{}{}
			result = append(result, member)
		}
	}

	// Add members from other sets
	for _, other := range others {
		for member := range other.data {
			if _, exists := seen[member]; !exists {
				seen[member] = struct{}{}
				result = append(result, member)
			}
		}
	}

	return result
}

// IsSubset checks if this set is a subset of another set
func (s *Set) IsSubset(other *Set) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	if len(s.data) > len(other.data) {
		return false
	}

	for member := range s.data {
		if _, exists := other.data[member]; !exists {
			return false
		}
	}

	return true
}

// Clear removes all members from the set
func (s *Set) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]struct{})
}

// Scan iterates over members with cursor
func (s *Set) Scan(cursor int, count int) (int, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]string, 0, len(s.data))
	for member := range s.data {
		members = append(members, member)
	}

	if cursor < 0 {
		cursor = 0
	}

	if cursor >= len(members) {
		return 0, nil
	}

	end := cursor + count
	if end > len(members) {
		end = len(members)
	}

	result := members[cursor:end]
	newCursor := end
	if newCursor >= len(members) {
		newCursor = 0
	}

	return newCursor, result
}

// Copy returns a copy of the set
func (s *Set) Copy() *Set {
	s.mu.RLock()
	defer s.mu.RUnlock()

	newSet := &Set{
		data:     make(map[string]struct{}, len(s.data)),
		encoding: s.encoding,
	}
	for member := range s.data {
		newSet.data[member] = struct{}{}
	}
	return newSet
}

// Encoding returns the set encoding type
func (s *Set) Encoding() SetEncoding {
	return s.encoding
}

// Size returns the approximate memory size
func (s *Set) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	size := int64(0)
	for member := range s.data {
		size += int64(len(member))
	}
	// Add overhead for map structure
	size += int64(len(s.data)) * 16
	return size
}

// ToSlice returns all members as a slice
func (s *Set) ToSlice() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]string, 0, len(s.data))
	for member := range s.data {
		result = append(result, member)
	}
	return result
}
