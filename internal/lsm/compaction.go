package lsm

import (
	"fmt"
	"os"

	"kv-engine/internal/model"
)

// sizeTieredCompaction implements size-tiered compaction strategy.
// At each level, if the number of SSTables reaches the threshold, all SSTables
// are merged into one and flushed to the next level.
// Merge operands belonging to deleted probabilistic instances are purged.
func (t *LSMTree) sizeTieredCompaction(deleted map[instanceKey]bool) error {
	lastLevel := t.cfg.MaxLevels - 1

	// Compact non-last levels: merge into next level.
	for lvl := 0; lvl < lastLevel; lvl++ {
		dir := t.levelDir(lvl)
		files, err := t.sst.ListDataFilesInDir(dir)
		if err != nil {
			return fmt.Errorf("size-tiered: list L%d: %w", lvl, err)
		}

		if len(files) < t.cfg.SizeTieredMinSSTables {
			continue
		}

		// Read all records from all SSTables in this level.
		var batches [][]model.Record
		for _, f := range files {
			recs, err := t.sst.ReadAllRecordsFromFile(f)
			if err != nil {
				return fmt.Errorf("size-tiered: read %s: %w", f, err)
			}
			batches = append(batches, recs)
		}

		merged := mergeAndDedup(batches)
		merged = filterDeletedOperands(merged, deleted)

		// Flush merged records to the next level.
		nextDir := t.levelDir(lvl + 1)
		if err := os.MkdirAll(nextDir, 0755); err != nil {
			return fmt.Errorf("size-tiered: mkdir L%d: %w", lvl+1, err)
		}

		if len(merged) > 0 {
			if err := t.sst.FlushToDir(nextDir, merged); err != nil {
				return fmt.Errorf("size-tiered: flush to L%d: %w", lvl+1, err)
			}
		}

		// Delete old SSTables from this level.
		for _, f := range files {
			if err := t.sst.DeleteSSTable(f); err != nil {
				return fmt.Errorf("size-tiered: delete %s: %w", f, err)
			}
		}
	}

	// Last-level in-place compaction: merge all SSTables into one, purge tombstones.
	if err := t.compactLastLevel(deleted); err != nil {
		return fmt.Errorf("size-tiered: last-level: %w", err)
	}

	return nil
}

// leveledCompaction implements leveled compaction strategy.
// L0: If the number of SSTables exceeds the threshold, all L0 SSTables are
//
//	merged with all L1 SSTables and the result is written to L1.
//
// L1+: If the total data size at a level exceeds the target size for that level,
//
//	all SSTables are merged with the next level.
//
// Merge operands belonging to deleted probabilistic instances are purged.
func (t *LSMTree) leveledCompaction(deleted map[instanceKey]bool) error {
	// Phase 1: L0 compaction
	l0Dir := t.levelDir(0)
	l0Files, err := t.sst.ListDataFilesInDir(l0Dir)
	if err != nil {
		return fmt.Errorf("leveled: list L0: %w", err)
	}

	if len(l0Files) >= t.cfg.LeveledL0Threshold {
		if err := t.compactLevel(0, deleted); err != nil {
			return fmt.Errorf("leveled: compact L0: %w", err)
		}
	}

	// Phase 2: L1+ compaction cascade
	for lvl := 1; lvl < t.cfg.MaxLevels-1; lvl++ {
		targetBytes := t.levelTargetSize(lvl)
		dir := t.levelDir(lvl)
		currentSize, err := t.sst.GetDirTotalSize(dir)
		if err != nil {
			return fmt.Errorf("leveled: size L%d: %w", lvl, err)
		}

		if currentSize > targetBytes {
			if err := t.compactLevel(lvl, deleted); err != nil {
				return fmt.Errorf("leveled: compact L%d: %w", lvl, err)
			}
		}
	}

	// Last-level in-place compaction.
	if err := t.compactLastLevel(deleted); err != nil {
		return fmt.Errorf("leveled: last-level: %w", err)
	}

	return nil
}

// levelTargetSize returns the target maximum size (in bytes) for the given level.
// L1 = LeveledBaseSizeMB * 1MB, L2 = L1 * multiplier, L3 = L2 * multiplier...
func (t *LSMTree) levelTargetSize(level int) int64 {
	base := int64(t.cfg.LeveledBaseSizeMB) * 1024 * 1024
	size := base
	for i := 1; i < level; i++ {
		size *= int64(t.cfg.LeveledMultiplier)
	}
	return size
}

// compactLevel merges all SSTables from the given level with all SSTables from
// the next level. The merged result is written to the next level and old SSTables
// from both levels are deleted.
func (t *LSMTree) compactLevel(level int, deleted map[instanceKey]bool) error {
	srcDir := t.levelDir(level)
	dstDir := t.levelDir(level + 1)

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("compact: mkdir L%d: %w", level+1, err)
	}

	srcFiles, err := t.sst.ListDataFilesInDir(srcDir)
	if err != nil {
		return fmt.Errorf("compact: list L%d: %w", level, err)
	}

	dstFiles, err := t.sst.ListDataFilesInDir(dstDir)
	if err != nil {
		return fmt.Errorf("compact: list L%d: %w", level+1, err)
	}

	// Read all records from source level
	var batches [][]model.Record
	for _, f := range srcFiles {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("compact: read src %s: %w", f, err)
		}
		batches = append(batches, recs)
	}

	// Read all records from destination level
	for _, f := range dstFiles {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("compact: read dst %s: %w", f, err)
		}
		batches = append(batches, recs)
	}

	merged := mergeAndDedup(batches)
	merged = filterDeletedOperands(merged, deleted)

	// Write merged result to destination level
	if len(merged) > 0 {
		if err := t.sst.FlushToDir(dstDir, merged); err != nil {
			return fmt.Errorf("compact: flush to L%d: %w", level+1, err)
		}
	}

	// Delete old source SSTables
	for _, f := range srcFiles {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("compact: delete src %s: %w", f, err)
		}
	}

	// Delete old destination SSTables
	for _, f := range dstFiles {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("compact: delete dst %s: %w", f, err)
		}
	}

	return nil
}

// compactLastLevel performs in-place compaction on the last level.
// When multiple SSTables exist at the last level, they are merged into one.
// Since there is no lower level, tombstones and expired records are purged.
func (t *LSMTree) compactLastLevel(deleted map[instanceKey]bool) error {
	lastLevel := t.cfg.MaxLevels - 1
	dir := t.levelDir(lastLevel)

	files, err := t.sst.ListDataFilesInDir(dir)
	if err != nil {
		return fmt.Errorf("last-level: list L%d: %w", lastLevel, err)
	}

	// Need at least 2 SSTables to compact.
	if len(files) < 2 {
		return nil
	}

	var batches [][]model.Record
	for _, f := range files {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("last-level: read %s: %w", f, err)
		}
		batches = append(batches, recs)
	}

	// Merge, purge tombstones + expired, and remove deleted instance operands.
	merged := mergeAndDedupPurge(batches)
	merged = filterDeletedOperands(merged, deleted)

	if len(merged) > 0 {
		if err := t.sst.FlushToDir(dir, merged); err != nil {
			return fmt.Errorf("last-level: flush L%d: %w", lastLevel, err)
		}
	}

	// Delete old SSTables.
	for _, f := range files {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("last-level: delete %s: %w", f, err)
		}
	}

	return nil
}
