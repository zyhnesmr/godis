// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zset

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// ZSetEncoding represents the encoding type of a sorted set
type ZSetEncoding byte

const (
	// ZSetEncodingSkiplist uses a skiplist + hashtable
	ZSetEncodingSkiplist ZSetEncoding = iota
	// ZSetEncodingZiplist uses a ziplist (TODO)
	ZSetEncodingZiplist
)

// ZMember represents a member with its score
type ZMember struct {
	Member string
	Score  float64
}

// ZSet represents a Redis sorted set data structure
// Uses a combination of skiplist (for range operations) and hash map (for O(1) lookups)
type ZSet struct {
	mu       sync.RWMutex
	dict     map[string]float64 // member -> score for O(1) lookups
	skiplist *SkipList           // for ordered operations
	encoding ZSetEncoding
}

// NewZSet creates a new sorted set
func NewZSet() *ZSet {
	return &ZSet{
		dict:     make(map[string]float64),
		skiplist: NewSkipList(),
		encoding: ZSetEncodingSkiplist,
	}
}

// Add adds or updates a member with the given score
// Returns the number of new members added (0 if updated, 1 if new)
func (z *ZSet) Add(member string, score float64) int {
	z.mu.Lock()
	defer z.mu.Unlock()

	_, exists := z.dict[member]

	// Update dict
	z.dict[member] = score

	// Update skiplist
	z.skiplist.Insert(member, score)

	if !exists {
		return 1
	}
	return 0
}

// AddMultiple adds or updates multiple members
// Returns the number of new members added
func (z *ZSet) AddMultiple(members []ZMember) int {
	z.mu.Lock()
	defer z.mu.Unlock()

	added := 0
	for _, m := range members {
		if _, exists := z.dict[m.Member]; !exists {
			added++
		}
		z.dict[m.Member] = m.Score
		z.skiplist.Insert(m.Member, m.Score)
	}

	return added
}

// Remove removes a member from the sorted set
// Returns true if the member was removed
func (z *ZSet) Remove(member string) bool {
	z.mu.Lock()
	defer z.mu.Unlock()

	score, exists := z.dict[member]
	if !exists {
		return false
	}

	delete(z.dict, member)
	z.skiplist.Delete(member, score)

	return true
}

// RemoveMultiple removes multiple members
// Returns the number of members removed
func (z *ZSet) RemoveMultiple(members []string) int {
	z.mu.Lock()
	defer z.mu.Unlock()

	removed := 0
	for _, member := range members {
		if score, exists := z.dict[member]; exists {
			delete(z.dict, member)
			z.skiplist.Delete(member, score)
			removed++
		}
	}

	return removed
}

// Score returns the score of a member
// Returns (0, false) if member doesn't exist
func (z *ZSet) Score(member string) (float64, bool) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	score, exists := z.dict[member]
	return score, exists
}

// ScoreMultiple returns scores for multiple members
func (z *ZSet) ScoreMultiple(members []string) []interface{} {
	z.mu.RLock()
	defer z.mu.RUnlock()

	result := make([]interface{}, len(members))
	for i, member := range members {
		if score, exists := z.dict[member]; exists {
			result[i] = score
		} else {
			result[i] = nil
		}
	}

	return result
}

// Rank returns the rank of a member (0-based, ascending by score)
// Returns -1 if member doesn't exist
func (z *ZSet) Rank(member string) int64 {
	z.mu.RLock()
	defer z.mu.RUnlock()

	score, exists := z.dict[member]
	if !exists {
		return -1
	}

	return z.skiplist.GetRank(member, score)
}

// RevRank returns the rank of a member (0-based, descending by score)
// Returns -1 if member doesn't exist
func (z *ZSet) RevRank(member string) int64 {
	z.mu.RLock()
	defer z.mu.RUnlock()

	rank := z.Rank(member)
	if rank == -1 {
		return -1
	}

	return int64(z.skiplist.Len()) - 1 - rank
}

// Range returns members in the rank range [start, end] (0-based, inclusive)
func (z *ZSet) Range(start, end int) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	nodes := z.skiplist.GetRangeByRank(start, end)
	result := make([]ZMember, len(nodes))
	for i, node := range nodes {
		result[i] = ZMember{Member: node.member, Score: node.score}
	}

	return result
}

// RangeWithScores returns members with scores in the rank range [start, end]
func (z *ZSet) RangeWithScores(start, end int) []ZMember {
	return z.Range(start, end)
}

// RevRange returns members in reverse rank range [start, end] (0-based, inclusive)
func (z *ZSet) RevRange(start, end int) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	nodes := z.skiplist.GetRangeByRank(start, end)
	result := make([]ZMember, len(nodes))

	// Reverse the result
	for i, node := range nodes {
		result[len(nodes)-1-i] = ZMember{Member: node.member, Score: node.score}
	}

	return result
}

// RangeByScore returns members in the score range [min, max]
func (z *ZSet) RangeByScore(min, max float64) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	nodes := z.skiplist.GetRangeByScore(min, max)
	result := make([]ZMember, len(nodes))
	for i, node := range nodes {
		result[i] = ZMember{Member: node.member, Score: node.score}
	}

	return result
}

// Count returns the number of members in the score range [min, max]
func (z *ZSet) Count(min, max float64) int {
	z.mu.RLock()
	defer z.mu.RUnlock()

	return int(z.skiplist.CountInRange(min, max))
}

// Len returns the number of members in the sorted set
func (z *ZSet) Len() int {
	z.mu.RLock()
	defer z.mu.RUnlock()

	return len(z.dict)
}

// IncrBy increments the score of a member by delta
// Returns the new score
func (z *ZSet) IncrBy(member string, delta float64) float64 {
	z.mu.Lock()
	defer z.mu.Unlock()

	newScore := delta
	if score, exists := z.dict[member]; exists {
		newScore = score + delta

		// Remove old node
		z.skiplist.Delete(member, score)
	}

	z.dict[member] = newScore
	z.skiplist.Insert(member, newScore)

	return newScore
}

// PopMax removes and returns the member with the highest score
func (z *ZSet) PopMax() (ZMember, bool) {
	z.mu.Lock()
	defer z.mu.Unlock()

	if len(z.dict) == 0 {
		return ZMember{}, false
	}

	node := z.skiplist.PopLast()
	if node == nil {
		return ZMember{}, false
	}

	delete(z.dict, node.member)

	return ZMember{Member: node.member, Score: node.score}, true
}

// PopMaxMultiple removes and returns multiple members with highest scores
func (z *ZSet) PopMaxMultiple(count int) []ZMember {
	z.mu.Lock()
	defer z.mu.Unlock()

	result := []ZMember{}
	for i := 0; i < count && len(z.dict) > 0; i++ {
		node := z.skiplist.PopLast()
		if node == nil {
			break
		}
		delete(z.dict, node.member)
		result = append(result, ZMember{Member: node.member, Score: node.score})
	}

	return result
}

// PopMin removes and returns the member with the lowest score
func (z *ZSet) PopMin() (ZMember, bool) {
	z.mu.Lock()
	defer z.mu.Unlock()

	if len(z.dict) == 0 {
		return ZMember{}, false
	}

	node := z.skiplist.PopFirst()
	if node == nil {
		return ZMember{}, false
	}

	delete(z.dict, node.member)

	return ZMember{Member: node.member, Score: node.score}, true
}

// PopMinMultiple removes and returns multiple members with lowest scores
func (z *ZSet) PopMinMultiple(count int) []ZMember {
	z.mu.Lock()
	defer z.mu.Unlock()

	result := []ZMember{}
	for i := 0; i < count && len(z.dict) > 0; i++ {
		node := z.skiplist.PopFirst()
		if node == nil {
			break
		}
		delete(z.dict, node.member)
		result = append(result, ZMember{Member: node.member, Score: node.score})
	}

	return result
}

// RemoveRangeByRank removes members in rank range [start, end]
// Returns the number of removed members
func (z *ZSet) RemoveRangeByRank(start, end int) int {
	z.mu.Lock()
	defer z.mu.Unlock()

	nodes := z.skiplist.GetRangeByRank(start, end)
	removed := 0

	for _, node := range nodes {
		if _, exists := z.dict[node.member]; exists {
			delete(z.dict, node.member)
			removed++
		}
		// Delete from skiplist
		z.skiplist.Delete(node.member, node.score)
	}

	return removed
}

// RemoveRangeByScore removes members in score range [min, max]
// Returns the number of removed members
func (z *ZSet) RemoveRangeByScore(min, max float64) int {
	z.mu.Lock()
	defer z.mu.Unlock()

	nodes := z.skiplist.GetRangeByScore(min, max)
	removed := 0

	for _, node := range nodes {
		if _, exists := z.dict[node.member]; exists {
			delete(z.dict, node.member)
			removed++
		}
		// Delete from skiplist
		z.skiplist.Delete(node.member, node.score)
	}

	return removed
}

// Members returns all members (without scores)
func (z *ZSet) Members() []string {
	z.mu.RLock()
	defer z.mu.RUnlock()

	members := make([]string, 0, len(z.dict))
	for member := range z.dict {
		members = append(members, member)
	}

	return members
}

// GetAll returns all members with scores
func (z *ZSet) GetAll() []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	nodes := z.skiplist.GetAll()
	result := make([]ZMember, len(nodes))
	for i, node := range nodes {
		result[i] = ZMember{Member: node.member, Score: node.score}
	}

	return result
}

// Scan iterates over members with cursor
func (z *ZSet) Scan(cursor int, count int) (int, []ZMember) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	nodes := z.skiplist.GetAll()

	if cursor < 0 {
		cursor = 0
	}

	if cursor >= len(nodes) {
		return 0, nil
	}

	end := cursor + count
	if end > len(nodes) {
		end = len(nodes)
	}

	result := make([]ZMember, 0, end-cursor)
	for i := cursor; i < end; i++ {
		result = append(result, ZMember{Member: nodes[i].member, Score: nodes[i].score})
	}

	newCursor := end
	if newCursor >= len(nodes) {
		newCursor = 0
	}

	return newCursor, result
}

// Clear removes all members from the sorted set
func (z *ZSet) Clear() {
	z.mu.Lock()
	defer z.mu.Unlock()

	z.dict = make(map[string]float64)
	z.skiplist = NewSkipList()
}

// Encoding returns the sorted set encoding type
func (z *ZSet) Encoding() ZSetEncoding {
	return z.encoding
}

// Size returns the approximate memory size
func (z *ZSet) Size() int64 {
	z.mu.RLock()
	defer z.mu.RUnlock()

	size := int64(0)
	for member := range z.dict {
		size += int64(len(member) + 8) // 8 bytes for float64
	}
	// Add overhead for map and skiplist
	size += int64(len(z.dict)) * 32
	return size
}

// Intersect computes the intersection with other sorted sets
// For intersection, we take the MIN score for members present in all sets
func (z *ZSet) Intersect(others []*ZSet, aggregate string) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	// Find common members
	if len(others) == 0 {
		return z.GetAll()
	}

	// Count occurrences and aggregate scores
	counts := make(map[string]int)
	scores := make(map[string]float64)

	for _, member := range z.Members() {
		counts[member] = 1
		scores[member] = z.dict[member]
	}

	for _, other := range others {
		for member, score := range other.dict {
			counts[member]++
			if _, exists := scores[member]; exists {
				switch aggregate {
				case "sum", "SUM":
					scores[member] += score
				case "min", "MIN":
					if score < scores[member] {
						scores[member] = score
					}
				case "max", "MAX":
					if score > scores[member] {
						scores[member] = score
					}
				default:
					// Default to sum
					scores[member] += score
				}
			}
		}
	}

	// Filter members present in all sets
	result := []ZMember{}
	for member, count := range counts {
		if count == len(others)+1 {
			result = append(result, ZMember{Member: member, Score: scores[member]})
		}
	}

	// Sort by score
	sortZMembers(result)

	return result
}

// Union computes the union with other sorted sets
func (z *ZSet) Union(others []*ZSet, aggregate string) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	scores := make(map[string]float64)

	// Add scores from this set
	for member, score := range z.dict {
		scores[member] = score
	}

	// Aggregate scores from other sets
	for _, other := range others {
		for member, score := range other.dict {
			if _, exists := scores[member]; exists {
				switch aggregate {
				case "sum", "SUM":
					scores[member] += score
				case "min", "MIN":
					if score < scores[member] {
						scores[member] = score
					}
				case "max", "MAX":
					if score > scores[member] {
						scores[member] = score
					}
				default:
					scores[member] += score
				}
			} else {
				scores[member] = score
			}
		}
	}

	// Convert to slice
	result := make([]ZMember, 0, len(scores))
	for member, score := range scores {
		result = append(result, ZMember{Member: member, Score: score})
	}

	// Sort by score
	sortZMembers(result)

	return result
}

// Diff computes the difference with other sorted sets
// Returns members in this set but not in others
func (z *ZSet) Diff(others []*ZSet) []ZMember {
	z.mu.RLock()
	defer z.mu.RUnlock()

	// Lock all other sets
	for _, other := range others {
		other.mu.RLock()
		defer other.mu.RUnlock()
	}

	// Build set of members to exclude
	exclude := make(map[string]bool)
	for _, other := range others {
		for member := range other.dict {
			exclude[member] = true
		}
	}

	// Filter and build result
	result := []ZMember{}
	for _, node := range z.skiplist.GetAll() {
		if !exclude[node.member] {
			result = append(result, ZMember{Member: node.member, Score: node.score})
		}
	}

	return result
}

// sortZMembers sorts members by score (ascending), then by member (lexicographic)
func sortZMembers(members []ZMember) {
	// Simple insertion sort (can be optimized with quicksort for large sets)
	for i := 1; i < len(members); i++ {
		for j := i; j > 0; j-- {
			if members[j].Score < members[j-1].Score ||
				(members[j].Score == members[j-1].Score && members[j].Member < members[j-1].Member) {
				members[j], members[j-1] = members[j-1], members[j]
			} else {
				break
			}
		}
	}
}

// ZMember represents a member-score pair for range operations
type rangeSpec struct {
	min   float64
	max   float64
	minEx bool // min is exclusive
	maxEx bool // max is exclusive
}

// parseRangeSpec parses a range specification
// Supported formats: "score", "(score", "-inf", "+inf"
func parseRangeSpec(minStr, maxStr string) (float64, float64, bool, bool) {
	var min, max float64
	minEx, maxEx := false, false

	switch minStr {
	case "-inf", "-Infinity":
		min = math.Inf(-1)
	case "(0":
		min = 0
		minEx = true
	default:
		if len(minStr) > 0 && minStr[0] == '(' {
			minEx = true
			minStr = minStr[1:]
		}
		min = parseFloat(minStr, math.Inf(-1))
	}

	switch maxStr {
	case "+inf", "Infinity", "inf":
		max = math.Inf(1)
	default:
		if len(maxStr) > 0 && maxStr[0] == '(' {
			maxEx = true
			maxStr = maxStr[1:]
		}
		max = parseFloat(maxStr, math.Inf(1))
	}

	return min, max, minEx, maxEx
}

func parseFloat(s string, defaultVal float64) float64 {
	// Parse float, return default if invalid
	if s == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return defaultVal
	}
	return f
}

// UpdateTime updates the access time
func (z *ZSet) UpdateTime() {
	// For compatibility with Object interface
	_ = time.Now()
}
