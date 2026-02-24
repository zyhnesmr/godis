package zset

import (
	"fmt"
	"testing"
)

func TestZSetBasic(t *testing.T) {
	zs := NewZSet()
	
	// Test Add
	added := zs.Add("one", 1.0)
	if added != 1 {
		t.Errorf("Add one:1 expected 1, got %d", added)
	}
	
	zs.Add("two", 2.0)
	zs.Add("three", 3.0)
	
	// Test Len
	if zs.Len() != 3 {
		t.Errorf("ZCARD expected 3, got %d", zs.Len())
	}
	
	// Test Score
	score, exists := zs.Score("two")
	if !exists || score != 2.0 {
		t.Errorf("ZSCORE two expected 2.0, got %f", score)
	}
	
	// Test Rank
	rank := zs.Rank("two")
	if rank != 1 {
		t.Errorf("ZRANK two expected 1, got %d", rank)
	}
	
	// Test Range
	members := zs.Range(0, -1)
	if len(members) != 3 {
		t.Errorf("ZRANGE expected 3 members, got %d", len(members))
	}
	
	// Test IncrBy
	newScore := zs.IncrBy("two", 2.0)
	if newScore != 4.0 {
		t.Errorf("ZINCRBY two 2 expected 4.0, got %f", newScore)
	}

	// After ZINCRBY, two has score 4.0 (highest), three has 3.0, one has 1.0

	// Test Count
	count := zs.Count(2, 5)
	if count != 2 { // two(4.0) and three(3.0) are in range
		t.Errorf("ZCOUNT 2 5 expected 2, got %d", count)
	}

	// Test PopMax - two now has score 4.0 (highest)
	maxMember, exists := zs.PopMax()
	if !exists || maxMember.Member != "two" || maxMember.Score != 4.0 {
		t.Errorf("ZPOPMAX expected two:4.0, got %s:%f", maxMember.Member, maxMember.Score)
	}

	// Test PopMax again - should get three
	maxMember, exists = zs.PopMax()
	if !exists || maxMember.Member != "three" {
		t.Errorf("ZPOPMAX (2nd) expected three, got %s", maxMember.Member)
	}

	// After 2 PopMax, only one remains
	if zs.Len() != 1 {
		t.Errorf("After 2 PopMax, expected ZCARD 1, got %d", zs.Len())
	}

	// Test Remove
	removed := zs.Remove("one")
	if !removed {
		t.Errorf("ZREM one failed")
	}

	// After removing one, zset should be empty
	if zs.Len() != 0 {
		t.Errorf("Final ZCARD expected 0, got %d", zs.Len())
	}
	
	fmt.Println("=== All ZSet tests passed! ===")
}
