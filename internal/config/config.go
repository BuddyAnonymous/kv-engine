package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	DataDir              string `json:"data_dir"`
	BlockSize            int    `json:"block_size"`
	MemtableMaxEntries   int    `json:"memtable_max_entries"`
	WALSegmentMaxRecords int    `json:"wal_segment_max_records"`
	MultiFileSSTable     bool   `json:"multi_file_sstable"`
	MemtableMaxBytes     int64  `json:"memtable_max_bytes"`
	MemtableType         string `json:"memtable_type"`
	BTreeDegree          int    `json:"btree_degree"`
	MemtableInstances    int    `json:"memtable_instances"`
}

func Default() Config {
	return Config{
		DataDir:              "data",
		BlockSize:            4096,
		MemtableMaxEntries:   1000,
		WALSegmentMaxRecords: 1000,
		MultiFileSSTable:     true,
		MemtableMaxBytes:     1024,
		MemtableType:         "hashmap",
		BTreeDegree:          16,
		MemtableInstances:    1,
	}
}

func (c *Config) Normalize() {
	d := Default()

	// DataDir
	if c.DataDir == "" {
		c.DataDir = d.DataDir
	}

	// BlockSize
	switch c.BlockSize {
	case 4096, 8192, 16384:
		// ok
	default:
		c.BlockSize = d.BlockSize
	}

	// MemtableMaxEntries
	if c.MemtableMaxEntries <= 0 {
		c.MemtableMaxEntries = d.MemtableMaxEntries
	}

	// WALSegmentMaxRecords
	if c.WALSegmentMaxRecords <= 0 {
		c.WALSegmentMaxRecords = d.WALSegmentMaxRecords
	}

	// MemtableMaxBytes
	if c.MemtableMaxBytes <= 0 {
		c.MemtableMaxBytes = d.MemtableMaxBytes
	}

	// MemtableType
	switch c.MemtableType {
	case "", "hashmap", "skiplist", "btree":
		if c.MemtableType == "" {
			c.MemtableType = d.MemtableType
		}
	default:
		c.MemtableType = d.MemtableType
	}

	// BTreeDegree: t mora biti >= 2
	if c.BTreeDegree < 2 {
		c.BTreeDegree = d.BTreeDegree
	}

	// MemtableInstances: mora biti >= 1
	if c.MemtableInstances < 1 {
		c.MemtableInstances = d.MemtableInstances
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, nil
	}

	// Unmarshal preko default-a znaci: sta fali u JSON-u ostaje default.
	if err := json.Unmarshal(b, &cfg); err != nil {
		fmt.Println("Greska u config.json, koristi se default config:", err)
		cfg = Default()
		cfg.Normalize()
		return cfg, nil
	}

	cfg.Normalize()
	return cfg, nil
}
