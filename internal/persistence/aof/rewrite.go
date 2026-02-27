// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aof

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/zyhnesmr/godis/internal/database"
	"github.com/zyhnesmr/godis/internal/datastruct/hash"
	"github.com/zyhnesmr/godis/internal/datastruct/list"
	"github.com/zyhnesmr/godis/internal/datastruct/set"
	"github.com/zyhnesmr/godis/internal/datastruct/zset"
	"github.com/zyhnesmr/godis/internal/protocol/resp"
)

// Rewrite performs an AOF rewrite
func (a *AOF) Rewrite(dbs []*database.DB) error {
	if a.rewriteInProgress.Load() {
		return fmt.Errorf("AOF rewrite already in progress")
	}

	a.rewriteInProgress.Store(true)
	defer func() {
		a.rewriteInProgress.Store(false)
		a.lastRewriteTime = time.Now()
	}()

	// Create temporary file
	tmpFilename := a.GetFilename() + ".rewrite.tmp"
	tmpFile, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("failed to create rewrite file: %w", err)
	}
	defer tmpFile.Close()

	// Create serializer
	builder := resp.NewResponseBuilder()

	// Rewrite all databases
	for dbIdx, db := range dbs {
		// Write SELECT command
		a.writeSelectCommand(builder, dbIdx)

		// Get all keys from this database
		keys := db.Keys("*")
		if len(keys) == 0 {
			continue
		}

		// Rewrite each key
		for _, key := range keys {
			if err := a.rewriteKey(db, builder, key); err != nil {
				return fmt.Errorf("failed to rewrite key %s: %w", key, err)
			}
		}
	}

	// Write buffer to file
	if _, err := tmpFile.Write(builder.Bytes()); err != nil {
		return fmt.Errorf("failed to write rewrite file: %w", err)
	}

	// Sync to disk
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync rewrite file: %w", err)
	}

	// Rename to final filename (atomic operation)
	finalFilename := a.GetFilename()
	if err := os.Rename(tmpFilename, finalFilename); err != nil {
		return fmt.Errorf("failed to rename rewrite file: %w", err)
	}

	// Update base size
	if info, err := os.Stat(finalFilename); err == nil {
		a.baseSize = info.Size()
	}

	return nil
}

// RewriteInBackground performs an AOF rewrite in the background
func (a *AOF) RewriteInBackground(dbs []*database.DB) chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		errChan <- a.Rewrite(dbs)
	}()

	return errChan
}

// rewriteKey rewrites a single key to the AOF file
func (a *AOF) rewriteKey(db *database.DB, builder *resp.ResponseBuilder, key string) error {
	// Get object
	obj, ok := db.Get(key)
	if !ok {
		return nil
	}

	// Get type and rewrite accordingly
	switch obj.Type {
	case database.ObjTypeString:
		return a.rewriteString(builder, key, obj)
	case database.ObjTypeList:
		return a.rewriteList(builder, key, obj)
	case database.ObjTypeSet:
		return a.rewriteSet(builder, key, obj)
	case database.ObjTypeHash:
		return a.rewriteHash(builder, key, obj)
	case database.ObjTypeZSet:
		return a.rewriteZSet(builder, key, obj)
	default:
		return fmt.Errorf("unknown object type: %s", obj.Type)
	}
}

// rewriteString rewrites a string key
func (a *AOF) rewriteString(builder *resp.ResponseBuilder, key string, obj *database.Object) error {
	var value string
	switch v := obj.Ptr.(type) {
	case string:
		value = v
	case []byte:
		value = string(v)
	case int64:
		value = strconv.FormatInt(v, 10)
	case float64:
		value = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		value = fmt.Sprintf("%v", v)
	}

	// Write SET command
	builder.WriteArray(3)
	builder.WriteBulkStringFromString("SET")
	builder.WriteBulkStringFromString(key)
	builder.WriteBulkStringFromString(value)

	return nil
}

// rewriteList rewrites a list key
func (a *AOF) rewriteList(builder *resp.ResponseBuilder, key string, obj *database.Object) error {
	l, ok := obj.Ptr.(*list.List)
	if !ok {
		return fmt.Errorf("not a list object")
	}

	// Get all elements
	elements := l.ToSlice()
	if len(elements) == 0 {
		// Empty list, use RPUSH
		builder.WriteArray(2)
		builder.WriteBulkStringFromString("RPUSH")
		builder.WriteBulkStringFromString(key)
		return nil
	}

	// Write RPUSH command with all elements
	builder.WriteArray(2 + len(elements))
	builder.WriteBulkStringFromString("RPUSH")
	builder.WriteBulkStringFromString(key)
	for _, elem := range elements {
		builder.WriteBulkStringFromString(elem)
	}

	return nil
}

// rewriteSet rewrites a set key
func (a *AOF) rewriteSet(builder *resp.ResponseBuilder, key string, obj *database.Object) error {
	s, ok := obj.Ptr.(*set.Set)
	if !ok {
		return fmt.Errorf("not a set object")
	}

	// Get all members
	members := s.Members()
	if len(members) == 0 {
		// Empty set, use SADD
		builder.WriteArray(2)
		builder.WriteBulkStringFromString("SADD")
		builder.WriteBulkStringFromString(key)
		return nil
	}

	// Write SADD command with all members
	builder.WriteArray(2 + len(members))
	builder.WriteBulkStringFromString("SADD")
	builder.WriteBulkStringFromString(key)
	for _, member := range members {
		builder.WriteBulkStringFromString(member)
	}

	return nil
}

// rewriteHash rewrites a hash key
func (a *AOF) rewriteHash(builder *resp.ResponseBuilder, key string, obj *database.Object) error {
	h, ok := obj.Ptr.(*hash.Hash)
	if !ok {
		return fmt.Errorf("not a hash object")
	}

	// Get all fields and values
	hashMap := h.GetAllMap()
	if len(hashMap) == 0 {
		// Empty hash, use HSET
		builder.WriteArray(2)
		builder.WriteBulkStringFromString("HSET")
		builder.WriteBulkStringFromString(key)
		return nil
	}

	// Write HSET command with all fields
	// HSET key field1 value1 field2 value2 ...
	args := make([]string, 0, len(hashMap)*2)
	for field, value := range hashMap {
		args = append(args, field, value)
	}

	builder.WriteArray(2 + len(args))
	builder.WriteBulkStringFromString("HSET")
	builder.WriteBulkStringFromString(key)
	for _, arg := range args {
		builder.WriteBulkStringFromString(arg)
	}

	return nil
}

// rewriteZSet rewrites a sorted set key
func (a *AOF) rewriteZSet(builder *resp.ResponseBuilder, key string, obj *database.Object) error {
	z, ok := obj.Ptr.(*zset.ZSet)
	if !ok {
		return fmt.Errorf("not a zset object")
	}

	// Get all members
	members := z.GetAll()
	if len(members) == 0 {
		// Empty zset, use ZADD
		builder.WriteArray(2)
		builder.WriteBulkStringFromString("ZADD")
		builder.WriteBulkStringFromString(key)
		return nil
	}

	// Write ZADD command with all members
	// ZADD key score1 member1 score2 member2 ...
	builder.WriteArray(2 + len(members)*2)
	builder.WriteBulkStringFromString("ZADD")
	builder.WriteBulkStringFromString(key)
	for _, m := range members {
		builder.WriteBulkStringFromString(strconv.FormatFloat(m.Score, 'f', -1, 64))
		builder.WriteBulkStringFromString(m.Member)
	}

	return nil
}

// writeSelectCommand writes a SELECT command
func (a *AOF) writeSelectCommand(builder *resp.ResponseBuilder, db int) {
	builder.WriteArray(2)
	builder.WriteBulkStringFromString("SELECT")
	builder.WriteBulkStringFromString(strconv.Itoa(db))
}

// RewriteProgress tracks the progress of an AOF rewrite
type RewriteProgress struct {
	mu           sync.Mutex
	BytesWritten int64
	TotalBytes   int64
	Done         bool
	Error        error
}

// RewriteWithProgress performs an AOF rewrite with progress tracking
func (a *AOF) RewriteWithProgress(dbs []*database.DB) (*RewriteProgress, error) {
	if a.rewriteInProgress.Load() {
		return nil, fmt.Errorf("AOF rewrite already in progress")
	}

	a.rewriteInProgress.Store(true)

	progress := &RewriteProgress{}
	progressBytes := make(chan int64)
	errChan := make(chan error, 1)

	// Start rewrite in background
	go func() {
		defer func() {
			a.rewriteInProgress.Store(false)
			a.lastRewriteTime = time.Now()
			close(progressBytes)
			close(errChan)
		}()

		// Create temporary file
		tmpFilename := a.GetFilename() + ".rewrite.tmp"
		tmpFile, err := os.Create(tmpFilename)
		if err != nil {
			progress.Error = fmt.Errorf("failed to create rewrite file: %w", err)
			errChan <- err
			return
		}
		defer tmpFile.Close()

		builder := resp.NewResponseBuilder()
		bytesWritten := int64(0)

		// Rewrite all databases
		for dbIdx, db := range dbs {
			a.writeSelectCommand(builder, dbIdx)
			bytesWritten += int64(len(builder.Bytes()))
			progressBytes <- bytesWritten
			builder.Reset()

			keys := db.Keys("*")
			for _, key := range keys {
				if err := a.rewriteKey(db, builder, key); err != nil {
					progress.Error = err
					errChan <- err
					return
				}
				bytesWritten += int64(len(builder.Bytes()))
				progressBytes <- bytesWritten
				builder.Reset()

				// Write to file periodically
				if bytesWritten > 32*1024 {
					if _, err := tmpFile.Write(builder.Bytes()); err != nil {
						progress.Error = err
						errChan <- err
						return
					}
					builder.Reset()
				}
			}
		}

		// Write remaining buffer
		if _, err := tmpFile.Write(builder.Bytes()); err != nil {
			progress.Error = err
			errChan <- err
			return
		}

		// Sync and rename
		if err := tmpFile.Sync(); err != nil {
			progress.Error = err
			errChan <- err
			return
		}

		finalFilename := a.GetFilename()
		if err := os.Rename(tmpFilename, finalFilename); err != nil {
			progress.Error = err
			errChan <- err
			return
		}

		if info, err := os.Stat(finalFilename); err == nil {
			a.baseSize = info.Size()
		}

		progress.Done = true
		errChan <- nil
	}()

	return progress, nil
}

// GetLastRewriteTime returns the time of the last rewrite
func (a *AOF) GetLastRewriteTime() time.Time {
	return a.lastRewriteTime
}

// IsRewriteInProgress returns true if a rewrite is in progress
func (a *AOF) IsRewriteInProgress() bool {
	return a.rewriteInProgress.Load()
}

// GetRewriteSize returns the size of the last/current rewrite
func (a *AOF) GetRewriteSize() int64 {
	return a.currentRewriteSize
}

// MultiDBRewrite handles rewriting with proper AOF format (database separator)
func (a *AOF) MultiDBRewrite(dbs []*database.DB) error {
	if a.rewriteInProgress.Load() {
		return fmt.Errorf("AOF rewrite already in progress")
	}

	a.rewriteInProgress.Store(true)
	defer func() {
		a.rewriteInProgress.Store(false)
		a.lastRewriteTime = time.Now()
	}()

	// Create temporary file
	tmpDir := filepath.Dir(a.GetFilename())
	tmpFilename := filepath.Join(tmpDir, "temp-rewrite.aof")
	tmpFile, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("failed to create rewrite file: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFilename)
	}()

	builder := resp.NewResponseBuilder()

	// Rewrite each database with separator
	for dbIdx, db := range dbs {
		// Get all keys
		keys := db.Keys("*")
		if len(keys) == 0 {
			continue
		}

		// Write SELECT command
		a.writeSelectCommand(builder, dbIdx)
		if _, err := tmpFile.Write(builder.Bytes()); err != nil {
			return err
		}
		builder.Reset()

		// Rewrite each key
		for _, key := range keys {
			if err := a.rewriteKey(db, builder, key); err != nil {
				return fmt.Errorf("failed to rewrite key %s: %w", key, err)
			}

			if _, err := tmpFile.Write(builder.Bytes()); err != nil {
				return err
			}
			builder.Reset()
		}
	}

	// Flush and sync
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync rewrite file: %w", err)
	}

	// Atomic rename
	finalFilename := a.GetFilename()
	if err := os.Rename(tmpFilename, finalFilename); err != nil {
		return fmt.Errorf("failed to rename rewrite file: %w", err)
	}

	// Update base size
	if info, err := os.Stat(finalFilename); err == nil {
		a.baseSize = info.Size()
	}

	return nil
}