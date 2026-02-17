package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	DataDir              string `json:"data_dir"`
	BlockSize            int    `json:"block_size"`
	SegmentBlocks        int    `json:"segment_blocks"`
	MemtableMaxEntries   int    `json:"memtable_max_entries"`
	WALSegmentMaxRecords int    `json:"wal_segment_max_records"`
	MultiFileSSTable     bool   `json:"multi_file_sstable"`
	MemtableMaxBytes     int64  `json:"memtable_max_bytes"`
	MemtableType         string `json:"memtable_type"`
	BTreeDegree          int    `json:"btree_degree"`
	MemtableInstances    int    `json:"memtable_instances"`
	CacheSize            int    `json:"cache_size"`
	BlockCacheSize       int    `json:"block_cache_size"`
	SummaryStride        uint   `json:"summary_stride"`

	// Probabilistic structure defaults (used on CREATE commands)
	BFExpectedElements  uint64  `json:"bf_expected_elements"`
	BFFalsePositiveRate float64 `json:"bf_false_positive_rate"`
	BFSeed              uint32  `json:"bf_seed"`
	CMSEpsilon          float64 `json:"cms_epsilon"`
	CMSDelta            float64 `json:"cms_delta"`
	CMSSeed             uint32  `json:"cms_seed"`
	HLLPrecision        uint8   `json:"hll_precision"`
	HLLSeed             uint32  `json:"hll_seed"`

	// LSM Tree params
	LSMMaxLevels              int    `json:"lsm_max_levels"`
	LSMCompactionAlgorithm    string `json:"lsm_compaction_algorithm"`
	LSMSizeTieredMinSSTables  int    `json:"lsm_size_tiered_min_sstables"`
	LSMLeveledL0Threshold     int    `json:"lsm_leveled_l0_threshold"`
	LSMLeveledBaseSizeMB      int    `json:"lsm_leveled_base_size_mb"`
	LSMLeveledMultiplier      int    `json:"lsm_leveled_multiplier"`
}

func Default() Config {
	return Config{
		DataDir:              "data",
		BlockSize:            4096,
		SegmentBlocks:        8,
		MemtableMaxEntries:   1000,
		WALSegmentMaxRecords: 1000,
		MultiFileSSTable:     true,
		MemtableMaxBytes:     1024,
		MemtableType:         "hashmap",
		BTreeDegree:          16,
		MemtableInstances:    1,
		CacheSize:            8192,
		BlockCacheSize:       8192,
		SummaryStride:        4,
		BFExpectedElements:   10000,
		BFFalsePositiveRate:  0.01,
		BFSeed:               0,
		CMSEpsilon:           0.001,
		CMSDelta:             0.01,
		CMSSeed:              0,
		HLLPrecision:         14,
		HLLSeed:              0,

		LSMMaxLevels:              4,
		LSMCompactionAlgorithm:    "size_tiered",
		LSMSizeTieredMinSSTables:  4,
		LSMLeveledL0Threshold:     4,
		LSMLeveledBaseSizeMB:      10,
		LSMLeveledMultiplier:      10,
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

	// CacheSize: mora biti >= 0
	if c.CacheSize <= 0 {
		c.CacheSize = d.CacheSize
	}

	// BlockCacheSize: mora biti >= 0
	if c.BlockCacheSize <= 0 {
		c.BlockCacheSize = d.BlockCacheSize
	}

	// SummaryStride: mora biti >= 1
	if c.SummaryStride < 1 {
		c.SummaryStride = d.SummaryStride
	}

	// BF params
	if c.BFExpectedElements == 0 {
		c.BFExpectedElements = d.BFExpectedElements
	}
	if c.BFFalsePositiveRate <= 0 || c.BFFalsePositiveRate >= 1 {
		c.BFFalsePositiveRate = d.BFFalsePositiveRate
	}

	// CMS params
	if c.CMSEpsilon <= 0 || c.CMSEpsilon >= 1 {
		c.CMSEpsilon = d.CMSEpsilon
	}
	if c.CMSDelta <= 0 || c.CMSDelta >= 1 {
		c.CMSDelta = d.CMSDelta
	}

	// HLL precision: practical range [4,18]
	if c.HLLPrecision < 4 || c.HLLPrecision > 18 {
		c.HLLPrecision = d.HLLPrecision
	}

	if c.SegmentBlocks <= 0 {
		c.SegmentBlocks = d.SegmentBlocks
	}

	// LSM params
	if c.LSMMaxLevels < 2 {
		c.LSMMaxLevels = d.LSMMaxLevels
	}
	switch c.LSMCompactionAlgorithm {
	case "size_tiered", "leveled":
		// ok
	default:
		c.LSMCompactionAlgorithm = d.LSMCompactionAlgorithm
	}
	if c.LSMSizeTieredMinSSTables < 2 {
		c.LSMSizeTieredMinSSTables = d.LSMSizeTieredMinSSTables
	}
	if c.LSMLeveledL0Threshold < 1 {
		c.LSMLeveledL0Threshold = d.LSMLeveledL0Threshold
	}
	if c.LSMLeveledBaseSizeMB < 1 {
		c.LSMLeveledBaseSizeMB = d.LSMLeveledBaseSizeMB
	}
	if c.LSMLeveledMultiplier < 2 {
		c.LSMLeveledMultiplier = d.LSMLeveledMultiplier
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
