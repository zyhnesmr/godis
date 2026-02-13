// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in LICENSE file.

package list

import (
	"sync"
)

// ListEncoding represents the encoding type of a list
type ListEncoding byte

const (
	// ListEncodingLinkedList uses a linked list
	ListEncodingLinkedList ListEncoding = iota
	// ListEncodingQuicklist uses a quicklist (linkedList + ziplist)
	ListEncodingQuicklist
)

// List represents a Redis list data structure
type List struct {
	mu       sync.RWMutex
	head     *listNode
	tail     *listNode
	length   int
	encoding ListEncoding
}

// listNode represents a node in linked list
type listNode struct {
	value string
	prev  *listNode
	next  *listNode
}

// NewList creates a new list
func NewList() *List {
	return &List{
		encoding: ListEncodingLinkedList,
	}
}

// Len returns the length of list
func (l *List) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.length
}

// PushLeft pushes a value to the left (head) of the list
func (l *List) PushLeft(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	node := &listNode{value: value}

	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	l.length++
}

// PushRight pushes a value to the right (tail) of the list
func (l *List) PushRight(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	node := &listNode{value: value}

	if l.tail == nil {
		l.head = node
		l.tail = node
	} else {
		node.prev = l.tail
		l.tail.next = node
		l.tail = node
	}
	l.length++
}

// PopLeft pops a value from the left (head) of the list
func (l *List) PopLeft() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head == nil {
		return "", false
	}

	value := l.head.value
	l.head = l.head.next
	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}
	l.length--
	return value, true
}

// PopRight pops a value from the right (tail) of the list
func (l *List) PopRight() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return "", false
	}

	value := l.tail.value
	l.tail = l.tail.prev
	if l.tail != nil {
		l.tail.next = nil
	} else {
		l.head = nil
	}
	l.length--
	return value, true
}

// Index returns the index of the first occurrence of a value
func (l *List) Index(index int) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < 0 || index >= l.length {
		return "", false
	}

	node := l.head
	for i := 0; i < index; i++ {
		if node == nil {
			return "", false
		}
		node = node.next
	}

	if node == nil {
		return "", false
	}
	return node.value, true
}

// Set sets a value at a given index
func (l *List) Set(index int, value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= l.length {
		return false
	}

	node := l.head
	for i := 0; i < index; i++ {
		if node == nil {
			return false
		}
		node = node.next
	}

	if node == nil {
		return false
	}
	node.value = value
	return true
}

// Range returns values from start to end (inclusive)
// Supports negative indices: -1 = last element, -2 = second to last, etc.
func (l *List) Range(start, end int) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	length := l.length
	if length == 0 {
		return []string{}
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = -1
		}
	}

	// Clamp indices to valid range
	if start >= length {
		return []string{}
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return []string{}
	}

	result := []string{}
	node := l.head
	for i := 0; node != nil && i <= end; i++ {
		if i >= start {
			result = append(result, node.value)
		}
		node = node.next
	}
	return result
}

// Trim removes elements from both ends
func (l *List) Trim(start, end int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	length := l.length
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

	if start >= length || start > end {
		return
	}

	if end >= length {
		end = length - 1
	}

	// Find new head node
	newHead := l.head
	for i := 0; i < start && newHead != nil; i++ {
		newHead = newHead.next
	}

	// Find new tail node
	newTail := newHead
	for i := start; i <= end && newTail != nil; i++ {
		newTail = newTail.next
	}
	if newTail != nil {
		newTail = newTail.prev
	} else if newHead != nil {
		for newTail = newHead; newTail.next != nil; newTail = newTail.next {
		}
	}

	l.head = newHead
	l.tail = newTail
	if l.head != nil {
		l.head.prev = nil
	}
	if l.tail != nil {
		l.tail.next = nil
	}
	l.length = end - start + 1
}

// Remove removes the first count occurrences of a value (count=0: remove all, count>0: remove first count, count<0: remove last count)
func (l *List) Remove(value string, count int) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	removed := 0

	if count >= 0 {
		// Remove from head
		node := l.head
		for node != nil && (count == 0 || removed < count) {
			next := node.next
			if node.value == value {
				// Remove node
				if node.prev != nil {
					node.prev.next = node.next
				} else {
					l.head = node.next
				}
				if node.next != nil {
					node.next.prev = node.prev
				} else {
					l.tail = node.prev
				}
				removed++
				l.length--
			}
			node = next
		}
	} else {
		// Remove from tail (negative count)
		count = -count
		node := l.tail
		for node != nil && removed < count {
			prev := node.prev
			if node.value == value {
				// Remove node
				if node.prev != nil {
					node.prev.next = node.next
				} else {
					l.head = node.next
				}
				if node.next != nil {
					node.next.prev = node.prev
				} else {
					l.tail = node.prev
				}
				removed++
				l.length--
			}
			node = prev
		}
	}

	return removed
}

// LPos returns the index of the first occurrence of a value
func (l *List) LPos(value string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	index := 0
	node := l.head
	for node != nil {
		if node.value == value {
			return index
		}
		node = node.next
		index++
	}
	return -1
}

// InsertBefore inserts a value before a pivot value
func (l *List) InsertBefore(pivot string, value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Find pivot node
	node := l.head
	for node != nil {
		if node.value == pivot {
			// Found pivot, insert before it
			newNode := &listNode{value: value, prev: node.prev, next: node}
			if node.prev != nil {
				node.prev.next = newNode
			} else {
				l.head = newNode
			}
			node.prev = newNode
			l.length++
			return true
		}
		node = node.next
	}
	return false
}

// InsertAfter inserts a value after a pivot value
func (l *List) InsertAfter(pivot string, value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Find pivot node
	node := l.head
	for node != nil {
		if node.value == pivot {
			// Found pivot, insert after it
			newNode := &listNode{value: value, prev: node, next: node.next}
			if node.next != nil {
				node.next.prev = newNode
			} else {
				l.tail = newNode
			}
			node.next = newNode
			l.length++
			return true
		}
		node = node.next
	}
	return false
}

// Clear removes all elements from list
func (l *List) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.head = nil
	l.tail = nil
	l.length = 0
}

// ToSlice returns all elements as a slice
func (l *List) ToSlice() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := []string{}
	node := l.head
	for node != nil {
		result = append(result, node.value)
		node = node.next
	}
	return result
}

// Encoding returns the list encoding type
func (l *List) Encoding() ListEncoding {
	return l.encoding
}

// Size returns approximate memory size
func (l *List) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	size := int64(l.length) * 16 // Base node overhead
	node := l.head
	for node != nil {
		size += int64(len(node.value))
		node = node.next
	}
	return size
}
