// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stream

import (
	"sync"
)

// RadixNode represents a node in the radix tree
type RadixNode struct {
	children map[byte]*RadixNode
	entry    *StreamEntry
}

// RadixTree is a radix tree for indexing stream entries by ID
// Simplified implementation: stores entry at the leaf node
type RadixTree struct {
	root *RadixNode
	mu   sync.RWMutex
}

// NewRadixTree creates a new radix tree
func NewRadixTree() *RadixTree {
	return &RadixTree{
		root: &RadixNode{
			children: make(map[byte]*RadixNode),
		},
	}
}

// Add adds an entry to the radix tree
func (t *RadixTree) Add(id StreamID, entry *StreamEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Convert ID to string for indexing
	key := id.String()

	node := t.root
	for _, c := range []byte(key) {
		if node.children[c] == nil {
			node.children[c] = &RadixNode{
				children: make(map[byte]*RadixNode),
			}
		}
		node = node.children[c]
	}

	// Store entry at leaf
	node.entry = entry
}

// Find finds an entry by ID
func (t *RadixTree) Find(id StreamID) *StreamEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	key := id.String()

	node := t.root
	for _, c := range []byte(key) {
		node = node.children[c]
		if node == nil {
			return nil
		}
	}

	return node.entry
}

// Delete removes an entry from the radix tree
func (t *RadixTree) Delete(id StreamID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := id.String()

	// Find the node to delete and its parent
	nodes := make([]*RadixNode, 0, len(key)+1)
	nodes[0] = t.root

	for _, c := range []byte(key) {
		parent := nodes[len(nodes)-1]
		node := parent.children[c]
		if node == nil {
			return false
		}
		nodes = append(nodes, node)
	}

	// Check if entry exists
	if nodes[len(nodes)-1].entry == nil {
		return false
	}

	// Remove entry
	nodes[len(nodes)-1].entry = nil

	// Clean up empty nodes (from leaf to root)
	for i := len(nodes) - 1; i > 0; i-- {
		node := nodes[i]
		parent := nodes[i-1]

		// Remove node if it has no entry and no children
		if node.entry == nil && len(node.children) == 0 {
			byteToRemove := []byte(key)[i-1]
			delete(parent.children, byteToRemove)
		} else {
			break
		}
	}

	return true
}

// Size returns the approximate memory size of the radix tree
func (t *RadixTree) Size() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.sizeRecursive(t.root, 0)
}

func (t *RadixTree) sizeRecursive(node *RadixNode, depth int) int64 {
	if node == nil {
		return 0
	}

	size := int64(depth) * 8 // Approximate pointer overhead

	for _, child := range node.children {
		size += t.sizeRecursive(child, depth+1)
	}

	if node.entry != nil {
		size += 64 // Approximate entry size
	}

	return size
}
