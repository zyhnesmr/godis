// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rdb

import (
	"fmt"
	"io"
	"os"

	"github.com/zyhnesmr/godis/internal/database"
)

// RDB manages RDB persistence
type RDB struct {
	dirname string
	dbname  string
}

// NewRDB creates a new RDB manager
func NewRDB(dirname, dbname string) *RDB {
	return &RDB{
		dirname: dirname,
		dbname:  dbname,
	}
}

// Save saves the database to RDB file
func (r *RDB) Save(dbs []*database.DB) error {
	// Ensure directory exists
	if err := os.MkdirAll(r.dirname, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Open file for writing
	filename := r.GetFilename()
	tmpFilename := filename + ".tmp"
	file, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create encoder and encode
	encoder := NewEncoder(file)
	if err := encoder.Encode(dbs); err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("failed to encode: %w", err)
	}

	// Rename to final filename (atomic operation)
	if err := os.Rename(tmpFilename, filename); err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}

// Load loads the database from RDB file
func (r *RDB) Load(dbs []*database.DB) error {
	filename := r.GetFilename()
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Clear all databases first
	for _, db := range dbs {
		db.FlushDB()
	}

	// Create decoder and decode
	decoder := NewDecoder(file)
	if err := decoder.Decode(dbs); err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}

	return nil
}

// SaveTo writes the database to a specific writer
func (r *RDB) SaveTo(w io.Writer, dbs []*database.DB) error {
	encoder := NewEncoder(w)
	return encoder.Encode(dbs)
}

// GetFilename returns the full path to the RDB file
func (r *RDB) GetFilename() string {
	return r.dirname + "/" + r.dbname
}

// FileSize returns the size of the RDB file in bytes
func (r *RDB) FileSize() (int64, error) {
	filename := r.GetFilename()
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// FileExists checks if the RDB file exists
func (r *RDB) FileExists() bool {
	filename := r.GetFilename()
	_, err := os.Stat(filename)
	return err == nil
}

// Delete removes the RDB file
func (r *RDB) Delete() error {
	filename := r.GetFilename()
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
