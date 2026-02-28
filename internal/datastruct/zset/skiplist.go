// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zset

import (
	"math"
	"math/rand/v2"
	"sync"
)

const (
	maxLevel    = 32
	probability = 0.25
)

// skipListNode represents a node in the skip list
type skipListNode struct {
	member   string
	score    float64
	forward  []*skipListNode
	span     []uint32 // span[i] is the distance to the next node at level i
	backward *skipListNode
}

// SkipList implements a skip list for sorted set
type SkipList struct {
	head   *skipListNode
	tail   *skipListNode
	length uint64
	level  int
	mu     sync.RWMutex
}

// newSkipListNode creates a new skip list node
func newSkipListNode(level int, member string, score float64) *skipListNode {
	return &skipListNode{
		member:  member,
		score:   score,
		forward: make([]*skipListNode, level),
		span:    make([]uint32, level),
	}
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	head := &skipListNode{
		member:  "",
		score:   math.Inf(-1),
		forward: make([]*skipListNode, maxLevel),
		span:    make([]uint32, maxLevel),
	}
	return &SkipList{
		head:  head,
		level: 1,
	}
}

// randomLevel returns a random level for a new node
func randomLevel() int {
	level := 1
	for rand.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// Insert inserts a new member with the given score
// If the member already exists, its score is updated
func (sl *SkipList) Insert(member string, score float64) *skipListNode {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipListNode, maxLevel)
	rank := make([]uint32, maxLevel)

	// Find the insertion position
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		rank[i] = rank[i+1] // Initialize rank from higher level
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].member < member)) {
			rank[i] += x.span[i]
			x = x.forward[i]
		}
		update[i] = x
	}

	// Check if member already exists
	if x.forward[0] != nil && x.forward[0].score == score && x.forward[0].member == member {
		// Member exists, update score
		x.forward[0].score = score
		return x.forward[0]
	}

	// Create new node
	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
			update[i].span[i] = uint64ToUint32(sl.length)
		}
		sl.level = level
	}

	x = newSkipListNode(level, member, score)

	// Insert node
	for i := 0; i < level; i++ {
		x.forward[i] = update[i].forward[i]
		update[i].forward[i] = x

		// Update span
		x.span[i] = update[i].span[i] - (rank[0] - rank[i])
		update[i].span[i] = rank[0] - rank[i] + 1
	}

	// Update span for higher levels
	for i := level; i < sl.level; i++ {
		update[i].span[i]++
	}

	// Set backward pointer
	if update[0] == sl.head {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.forward[0] != nil {
		x.forward[0].backward = x
	} else {
		sl.tail = x
	}

	sl.length++
	return x
}

// Delete removes a member from the skip list
// Returns true if the member was found and removed
func (sl *SkipList) Delete(member string, score float64) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipListNode, maxLevel)

	// Find the node to delete
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].member < member)) {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]

	// Check if we found the right node
	if x == nil || x.score != score || x.member != member {
		return false
	}

	// Remove node from all levels
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] != x {
			update[i].span[i]-- // Decrease span since we're removing a node
			continue
		}
		update[i].span[i] += x.span[i] - 1
		update[i].forward[i] = x.forward[i]
	}

	// Update tail and backward pointer
	if x.forward[0] != nil {
		x.forward[0].backward = x.backward
	} else {
		sl.tail = x.backward
	}

	// Decrease level if needed
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// Find finds a node by member
func (sl *SkipList) Find(member string, score float64) *skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].member < member)) {
			x = x.forward[i]
		}
	}

	x = x.forward[0]
	if x != nil && x.score == score && x.member == member {
		return x
	}

	return nil
}

// GetRank returns the rank of a member (0-based)
// Returns -1 if member not found
func (sl *SkipList) GetRank(member string, score float64) int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	rank := uint64(0)
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].member < member)) {
			rank += uint64(x.span[i])
			x = x.forward[i]
		}

		if x.forward[i] != nil && x.forward[i].member == member && x.forward[i].score == score {
			// Return 0-based rank: rank + span - 1
			return int64(rank + uint64(x.span[i]) - 1)
		}
	}

	return -1
}

// GetByRank returns the node at the given rank (0-based)
func (sl *SkipList) GetByRank(rank int) *skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if rank < 0 || int(rank) >= int(sl.length) {
		return nil
	}

	traversed := uint64(0)
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && traversed+uint64(x.span[i]) <= uint64(rank) {
			traversed += uint64(x.span[i])
			x = x.forward[i]
		}

		if traversed == uint64(rank)+1 {
			return x
		}
	}

	return x.forward[0]
}

// GetRangeByScore returns nodes in the given score range [min, max]
func (sl *SkipList) GetRangeByScore(min, max float64) []*skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Find the first node with score >= min
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].score < min {
			x = x.forward[i]
		}
	}

	x = x.forward[0]

	// Collect nodes
	result := []*skipListNode{}
	for x != nil && x.score <= max {
		result = append(result, x)
		x = x.forward[0]
	}

	return result
}

// GetRangeByRank returns nodes in the given rank range [start, end] (0-based, inclusive)
func (sl *SkipList) GetRangeByRank(start, end int) []*skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Handle negative indices
	if start < 0 {
		start = int(sl.length) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = int(sl.length) + end
		if end < 0 {
			return []*skipListNode{}
		}
	}

	// Clamp to valid range
	if start >= int(sl.length) {
		return []*skipListNode{}
	}
	if end >= int(sl.length) {
		end = int(sl.length) - 1
	}
	if start > end {
		return []*skipListNode{}
	}

	// Find the start node
	traversed := uint64(0)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && traversed+uint64(x.span[i]) <= uint64(start) {
			traversed += uint64(x.span[i])
			x = x.forward[i]
		}
	}

	// Collect nodes
	result := []*skipListNode{}
	x = x.forward[0]
	for i := 0; x != nil && i <= (end-start); i++ {
		result = append(result, x)
		x = x.forward[0]
	}

	return result
}

// CountInRange returns the number of nodes in the given score range [min, max]
func (sl *SkipList) CountInRange(min, max float64) uint64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Find the first node with score >= min
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].score < min {
			x = x.forward[i]
		}
	}

	x = x.forward[0]

	// Count nodes
	count := uint64(0)
	for x != nil && x.score <= max {
		count++
		x = x.forward[0]
	}

	return count
}

// Len returns the number of elements in the skip list
func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return int(sl.length)
}

// First returns the first node in the skip list
func (sl *SkipList) First() *skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.head.forward[0]
}

// Last returns the last node in the skip list
func (sl *SkipList) Last() *skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.tail
}

// PopFirst removes and returns the first node
func (sl *SkipList) PopFirst() *skipListNode {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	x := sl.head.forward[0]
	if x == nil {
		return nil
	}

	// Remove node
	for i := 0; i < sl.level; i++ {
		if sl.head.forward[i] != x {
			sl.head.span[i]--
			continue
		}
		sl.head.span[i] += x.span[i] - 1
		sl.head.forward[i] = x.forward[i]
	}

	// Update tail and backward
	if x.forward[0] != nil {
		x.forward[0].backward = nil
	} else {
		sl.tail = nil
	}

	// Decrease level
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.length--
	return x
}

// PopLast removes and returns the last node
func (sl *SkipList) PopLast() *skipListNode {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.tail == nil {
		return nil
	}

	x := sl.tail

	// Find the node before tail
	update := make([]*skipListNode, maxLevel)
	prev := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for prev.forward[i] != nil && prev.forward[i] != x {
			prev = prev.forward[i]
		}
		update[i] = prev
	}

	// Remove node
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] != x {
			update[i].span[i]--
			continue
		}
		update[i].span[i] += x.span[i] - 1
		update[i].forward[i] = x.forward[i]
	}

	// Update tail
	if x.backward != nil {
		x.backward.forward[0] = nil
		sl.tail = x.backward
	} else {
		sl.tail = nil
	}

	// Decrease level
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.length--
	return x
}

// GetAll returns all nodes in order
func (sl *SkipList) GetAll() []*skipListNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	result := []*skipListNode{}
	x := sl.head.forward[0]
	for x != nil {
		result = append(result, x)
		x = x.forward[0]
	}

	return result
}

func uint64ToUint32(v uint64) uint32 {
	if v > 4294967295 {
		return 4294967295
	}
	return uint32(v)
}
