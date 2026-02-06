package config

import (
	"encoding/json"
	"errors"
	"os"
)

type Config struct {
	DataDir              string `json:"data_dir"`
	BlockSize            int    `json:"block_size"`
	MemtableMaxEntries   int    `json:"memtable_max_entries"`
	WALSegmentMaxRecords int    `json:"wal_segment_max_records"`
}

func Default() Config {
	return Config{
		DataDir:              "data",
		BlockSize:            4096,
		MemtableMaxEntries:   1000,
		WALSegmentMaxRecords: 1000,
	}
}

func (c Config) Validate() error {
	if c.DataDir == "" {
		return errors.New("data_dir is empty")
	}
	if c.BlockSize != 4096 && c.BlockSize != 8192 && c.BlockSize != 16384 {
		return errors.New("block_size must be 4096, 8192, or 16384")
	}
	if c.MemtableMaxEntries <= 0 {
		return errors.New("memtable_max_entries must be > 0")
	}
	if c.WALSegmentMaxRecords <= 0 {
		return errors.New("wal_segment_max_records must be > 0")
	}
	return nil
}

func Load(path string) (Config, error) {
	cfg := Default()

	b, err := os.ReadFile(path)
	if err != nil {
		// nema fajla -> default
		if err2 := cfg.Validate(); err2 != nil {
			return Config{}, err2
		}
		return cfg, nil
	}

	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
