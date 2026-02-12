// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds the server configuration
type Config struct {
	// Network configuration
	Bind         string
	Port         int
	Timeout      int // 0 = no timeout
	TCPKeepalive int

	// General configuration
	Daemonize string // "yes" or "no"
	PidFile   string
	LogLevel  string
	LogFile   string
	Databases int

	// Snapshot configuration
	SaveRules               []SaveRule
	StopWritesOnBgsaveError bool
	RdbCompression          bool
	RdbChecksum             bool
	RdbFilename             string
	Dir                     string

	// Limits configuration
	MaxClients       int64
	MaxMemory        int64
	MaxMemoryPolicy  string
	MaxMemorySamples int

	// AOF configuration
	AppendOnly               string
	AppendFilename           string
	AppendFsync              string
	NoAppendfsyncOnRewrite   bool
	AutoAofRewritePercentage int
	AutoAofRewriteMinSize    int64

	// Slow query configuration
	SlowLogLogSlowerThan int64
	SlowLogMaxLen        int64

	// Advanced configuration for data structure encoding
	HashMaxZiplistEntries int
	HashMaxZiplistValue   int
	ListMaxZiplistSize    int
	ListCompressDepth     int
	SetMaxIntsetEntries   int
	ZSetMaxZiplistEntries int
	ZSetMaxZiplistValue   int

	mu sync.RWMutex
}

// SaveRule defines when to save the RDB file
type SaveRule struct {
	Seconds int
	Changes int
}

// Default returns the default configuration
func Default() *Config {
	return &Config{
		// Network
		Bind:         "0.0.0.0",
		Port:         6379,
		Timeout:      0,
		TCPKeepalive: 300,

		// General
		Daemonize: "no",
		PidFile:   "/var/run/godis.pid",
		LogLevel:  "notice",
		LogFile:   "",
		Databases: 16,

		// Snapshot
		SaveRules: []SaveRule{
			{Seconds: 900, Changes: 1},
			{Seconds: 300, Changes: 10},
			{Seconds: 60, Changes: 10000},
		},
		StopWritesOnBgsaveError: true,
		RdbCompression:          true,
		RdbChecksum:             true,
		RdbFilename:             "dump.rdb",
		Dir:                     ".",

		// Limits
		MaxClients:       10000,
		MaxMemory:        0,
		MaxMemoryPolicy:  "noeviction",
		MaxMemorySamples: 5,

		// AOF
		AppendOnly:               "no",
		AppendFilename:           "appendonly.aof",
		AppendFsync:              "everysec",
		NoAppendfsyncOnRewrite:   false,
		AutoAofRewritePercentage: 100,
		AutoAofRewriteMinSize:    64 << 20, // 64MB

		// Slow query
		SlowLogLogSlowerThan: 10000, // microseconds
		SlowLogMaxLen:        128,

		// Advanced
		HashMaxZiplistEntries: 512,
		HashMaxZiplistValue:   64,
		ListMaxZiplistSize:    -2,
		ListCompressDepth:     0,
		SetMaxIntsetEntries:   512,
		ZSetMaxZiplistEntries: 128,
		ZSetMaxZiplistValue:   64,
	}
}

// Global configuration instance
var globalConfig *Config
var once sync.Once

// Instance returns the global configuration instance
func Instance() *Config {
	once.Do(func() {
		globalConfig = Default()
	})
	return globalConfig
}

// ParseFlags parses command line flags
func (c *Config) ParseFlags() {
	configFile := flag.String("c", "", "Configuration file path")
	port := flag.Int("p", 0, "Server port")
	daemonize := flag.Bool("d", false, "Run as daemon")
	flag.Parse()

	if *port != 0 {
		c.Port = *port
	}
	if *daemonize {
		c.Daemonize = "yes"
	}
	if *configFile != "" {
		if err := c.LoadFile(*configFile); err != nil {
			fmt.Printf("Failed to load config file: %v\n", err)
			os.Exit(1)
		}
	}
}

// LoadFile loads configuration from a file
func (c *Config) LoadFile(filename string) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return c.Parse(string(content))
}

// Parse parses configuration content
func (c *Config) Parse(content string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lines := strings.Split(content, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Remove inline comments
		if idx := strings.Index(line, "#"); idx > 0 {
			line = strings.TrimSpace(line[:idx])
		}
		// Split key and value
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(parts[0])
		value := strings.Join(parts[1:], " ")

		if err := c.setConfig(key, value); err != nil {
			return fmt.Errorf("line %d: %w", i+1, err)
		}
	}
	return nil
}

// setConfig sets a single configuration value
func (c *Config) setConfig(key, value string) error {
	switch key {
	case "bind":
		c.Bind = value
	case "port":
		p, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.Port = p
	case "timeout":
		t, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.Timeout = t
	case "tcp-keepalive":
		k, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.TCPKeepalive = k
	case "daemonize":
		c.Daemonize = strings.ToLower(value)
	case "pidfile":
		c.PidFile = value
	case "loglevel":
		c.LogLevel = strings.ToLower(value)
	case "logfile":
		c.LogFile = value
	case "databases":
		d, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.Databases = d
	case "save":
		// Format: save <seconds> <changes>
		parts := strings.Fields(value)
		if len(parts) != 2 {
			return fmt.Errorf("invalid save format")
		}
		seconds, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}
		changes, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		c.SaveRules = append(c.SaveRules, SaveRule{Seconds: seconds, Changes: changes})
	case "stop-writes-on-bgsave-error":
		c.StopWritesOnBgsaveError = strings.ToLower(value) == "yes"
	case "rdbcompression":
		c.RdbCompression = strings.ToLower(value) == "yes"
	case "rdbchecksum":
		c.RdbChecksum = strings.ToLower(value) == "yes"
	case "dbfilename":
		c.RdbFilename = value
	case "dir":
		c.Dir = value
	case "maxclients":
		m, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		c.MaxClients = m
	case "maxmemory":
		if value == "0" || value == "" {
			c.MaxMemory = 0
		} else {
			m, err := parseMemory(value)
			if err != nil {
				return err
			}
			c.MaxMemory = m
		}
	case "maxmemory-policy":
		c.MaxMemoryPolicy = strings.ToLower(value)
	case "maxmemory-samples":
		s, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.MaxMemorySamples = s
	case "appendonly":
		c.AppendOnly = strings.ToLower(value)
	case "appendfilename":
		c.AppendFilename = value
	case "appendfsync":
		c.AppendFsync = strings.ToLower(value)
	case "no-appendfsync-on-rewrite":
		c.NoAppendfsyncOnRewrite = strings.ToLower(value) == "yes"
	case "auto-aof-rewrite-percentage":
		p, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.AutoAofRewritePercentage = p
	case "auto-aof-rewrite-min-size":
		s, err := parseMemory(value)
		if err != nil {
			return err
		}
		c.AutoAofRewriteMinSize = s
	case "slowlog-log-slower-than":
		s, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		c.SlowLogLogSlowerThan = s
	case "slowlog-max-len":
		s, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		c.SlowLogMaxLen = s
	case "hash-max-ziplist-entries":
		h, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.HashMaxZiplistEntries = h
	case "hash-max-ziplist-value":
		h, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.HashMaxZiplistValue = h
	case "list-max-ziplist-size":
		l, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.ListMaxZiplistSize = l
	case "list-compress-depth":
		l, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.ListCompressDepth = l
	case "set-max-intset-entries":
		s, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.SetMaxIntsetEntries = s
	case "zset-max-ziplist-entries":
		z, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.ZSetMaxZiplistEntries = z
	case "zset-max-ziplist-value":
		z, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		c.ZSetMaxZiplistValue = z
	default:
		// Unknown config key, ignore
	}
	return nil
}

// parseMemory parses memory size strings like "1gb", "500mb", etc.
func parseMemory(s string) (int64, error) {
	s = strings.ToLower(s)
	multiplier := int64(1)
	if strings.HasSuffix(s, "gb") {
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "gb")
	} else if strings.HasSuffix(s, "mb") {
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "mb")
	} else if strings.HasSuffix(s, "kb") {
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "kb")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

// Get returns a configuration value by key (for CONFIG GET command)
func (c *Config) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch strings.ToLower(key) {
	case "bind":
		return c.Bind, true
	case "port":
		return strconv.Itoa(c.Port), true
	case "timeout":
		return strconv.Itoa(c.Timeout), true
	case "tcp-keepalive":
		return strconv.Itoa(c.TCPKeepalive), true
	case "daemonize":
		return c.Daemonize, true
	case "pidfile":
		return c.PidFile, true
	case "loglevel":
		return c.LogLevel, true
	case "logfile":
		return c.LogFile, true
	case "databases":
		return strconv.Itoa(c.Databases), true
	case "save":
		var rules []string
		for _, r := range c.SaveRules {
			rules = append(rules, fmt.Sprintf("%d %d", r.Seconds, r.Changes))
		}
		return strings.Join(rules, " "), true
	case "stop-writes-on-bgsave-error":
		return boolToStr(c.StopWritesOnBgsaveError), true
	case "rdbcompression":
		return boolToStr(c.RdbCompression), true
	case "rdbchecksum":
		return boolToStr(c.RdbChecksum), true
	case "dbfilename":
		return c.RdbFilename, true
	case "dir":
		return c.Dir, true
	case "maxclients":
		return strconv.FormatInt(c.MaxClients, 10), true
	case "maxmemory":
		return strconv.FormatInt(c.MaxMemory, 10), true
	case "maxmemory-policy":
		return c.MaxMemoryPolicy, true
	case "maxmemory-samples":
		return strconv.Itoa(c.MaxMemorySamples), true
	case "appendonly":
		return c.AppendOnly, true
	case "appendfilename":
		return c.AppendFilename, true
	case "appendfsync":
		return c.AppendFsync, true
	case "slowlog-log-slower-than":
		return strconv.FormatInt(c.SlowLogLogSlowerThan, 10), true
	case "slowlog-max-len":
		return strconv.FormatInt(c.SlowLogMaxLen, 10), true
	default:
		return "", false
	}
}

// Set sets a configuration value by key (for CONFIG SET command)
func (c *Config) Set(key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setConfig(key, value)
}

// boolToStr converts boolean to "yes" or "no"
func boolToStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// IsDebugEnabled returns true if log level is debug
func (c *Config) IsDebugEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LogLevel == "debug"
}

// IsVerboseEnabled returns true if log level is verbose or debug
func (c *Config) IsVerboseEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LogLevel == "verbose" || c.LogLevel == "debug"
}

// GetAddr returns the network address to bind to
func (c *Config) GetAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Sprintf("%s:%d", c.Bind, c.Port)
}

// GetRdbPath returns the full path to the RDB file
func (c *Config) GetRdbPath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Dir + "/" + c.RdbFilename
}

// GetAofPath returns the full path to the AOF file
func (c *Config) GetAofPath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Dir + "/" + c.AppendFilename
}

// ShouldSave returns true if RDB save should be triggered based on save rules
func (c *Config) ShouldSave(lastSaveTime time.Time, changesSinceSave int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elapsed := time.Since(lastSaveTime)
	for _, rule := range c.SaveRules {
		if elapsed.Seconds() >= float64(rule.Seconds) && changesSinceSave >= rule.Changes {
			return true
		}
	}
	return false
}
