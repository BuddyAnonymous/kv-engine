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
func (t *LSMTree) sizeTieredCompaction(deleted map[instanceKey]bool, epochBoundary map[instanceKey]uint64) error {
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
		merged = filterExpiredRecords(merged)
		merged = filterStaleOperands(merged, deleted, epochBoundary)

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
	if err := t.compactLastLevel(deleted, epochBoundary); err != nil {
		return fmt.Errorf("size-tiered: last-level: %w", err)
	}

	return nil
}

// leveledCompaction implements leveled compaction strategy.
// L0: If the number of SSTables exceeds the threshold, all L0 SSTables are
//
//	merged with overlapping L1 SSTables and the result is written to L1
//	as multiple SSTables chunked by MaxSSTableSizeMB.
//
// L1+: If the total data size at a level exceeds the target size for that level,
//
//	the oldest SSTable is picked, merged with overlapping SSTables on the next level,
//	and the result is written as chunked SSTables to the next level.
//
// Merge operands belonging to deleted probabilistic instances are purged.
func (t *LSMTree) leveledCompaction(deleted map[instanceKey]bool, epochBoundary map[instanceKey]uint64) error {
	// Phase 1: L0 compaction — merge ALL L0 SSTables with overlapping L1 SSTables.
	l0Dir := t.levelDir(0)
	l0Files, err := t.sst.ListDataFilesInDir(l0Dir)
	if err != nil {
		return fmt.Errorf("leveled: list L0: %w", err)
	}

	if len(l0Files) >= t.cfg.LeveledL0Threshold {
		if err := t.compactL0(deleted, epochBoundary); err != nil {
			return fmt.Errorf("leveled: compact L0: %w", err)
		}
	}

	// Phase 2: L1+ compaction — pick oldest SSTable, merge with overlapping on L+1.
	// Repeat compaction at each level until its size is within the target,
	// since a single compaction may not be enough to bring it under the limit.
	for lvl := 1; lvl < t.cfg.MaxLevels-1; lvl++ {
		targetBytes := t.levelTargetSize(lvl)
		dir := t.levelDir(lvl)

		for {
			currentSize, err := t.sst.GetDirTotalSize(dir)
			if err != nil {
				return fmt.Errorf("leveled: size L%d: %w", lvl, err)
			}
			if currentSize <= targetBytes {
				break
			}
			if err := t.compactLeveledSingle(lvl, deleted, epochBoundary); err != nil {
				return fmt.Errorf("leveled: compact L%d: %w", lvl, err)
			}
		}
	}

	// Last-level in-place compaction.
	if err := t.compactLastLevel(deleted, epochBoundary); err != nil {
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

// maxSSTableBytes returns the configured max SSTable size in bytes.
func (t *LSMTree) maxSSTableBytes() int64 {
	return int64(t.cfg.MaxSSTableSizeMB) * 1024 * 1024
}

// compactL0 merges ALL L0 SSTables with overlapping L1 SSTables.
// L0 SSTables may have overlapping key ranges (they come from memtable flushes),
// so we must merge all of them together. We compute the combined key range of all
// L0 SSTables, find overlapping SSTables on L1, merge everything, and write the
// result as chunked SSTables to L1.
func (t *LSMTree) compactL0(deleted map[instanceKey]bool, epochBoundary map[instanceKey]uint64) error {
	l0Dir := t.levelDir(0)
	l1Dir := t.levelDir(1)

	if err := os.MkdirAll(l1Dir, 0755); err != nil {
		return fmt.Errorf("compactL0: mkdir L1: %w", err)
	}

	l0Files, err := t.sst.ListDataFilesInDir(l0Dir)
	if err != nil {
		return fmt.Errorf("compactL0: list L0: %w", err)
	}
	if len(l0Files) == 0 {
		return nil
	}

	// Read all L0 records AND compute combined key range.
	var batches [][]model.Record
	var globalMin, globalMax string
	for _, f := range l0Files {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("compactL0: read L0 %s: %w", f, err)
		}
		batches = append(batches, recs)

		fMin, fMax, err := t.sst.GetKeyRangeForFile(f)
		if err != nil {
			// Fallback: scan records for min/max
			for _, r := range recs {
				if globalMin == "" || r.Key < globalMin {
					globalMin = r.Key
				}
				if globalMax == "" || r.Key > globalMax {
					globalMax = r.Key
				}
			}
			continue
		}
		if globalMin == "" || fMin < globalMin {
			globalMin = fMin
		}
		if globalMax == "" || fMax > globalMax {
			globalMax = fMax
		}
	}

	// Find overlapping L1 SSTables.
	overlapping, err := t.sst.FindOverlappingFiles(l1Dir, globalMin, globalMax)
	if err != nil {
		return fmt.Errorf("compactL0: find overlapping L1: %w", err)
	}

	// Read overlapping L1 records.
	for _, f := range overlapping {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("compactL0: read L1 %s: %w", f, err)
		}
		batches = append(batches, recs)
	}

	merged := mergeAndDedup(batches)
	merged = filterExpiredRecords(merged)
	merged = filterStaleOperands(merged, deleted, epochBoundary)

	// Write merged result as chunked SSTables to L1.
	if len(merged) > 0 {
		if err := t.sst.FlushToDirChunked(l1Dir, merged, t.maxSSTableBytes()); err != nil {
			return fmt.Errorf("compactL0: flush to L1: %w", err)
		}
	}

	// Delete old L0 SSTables.
	for _, f := range l0Files {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("compactL0: delete L0 %s: %w", f, err)
		}
	}

	// Delete old overlapping L1 SSTables.
	for _, f := range overlapping {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("compactL0: delete L1 %s: %w", f, err)
		}
	}

	return nil
}

// compactLeveledSingle picks the oldest SSTable from the given level, finds
// overlapping SSTables on the next level, merges them, and writes the result
// as chunked SSTables to the next level.
func (t *LSMTree) compactLeveledSingle(level int, deleted map[instanceKey]bool, epochBoundary map[instanceKey]uint64) error {
	srcDir := t.levelDir(level)
	dstDir := t.levelDir(level + 1)

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("compact L%d: mkdir L%d: %w", level, level+1, err)
	}

	// Pick the oldest SSTable on this level.
	oldest, err := t.sst.FindOldestFile(srcDir)
	if err != nil {
		return fmt.Errorf("compact L%d: find oldest: %w", level, err)
	}
	if oldest == "" {
		return nil
	}

	// Read key range from the oldest SSTable's summary.
	minKey, maxKey, err := t.sst.GetKeyRangeForFile(oldest)
	if err != nil {
		return fmt.Errorf("compact L%d: key range %s: %w", level, oldest, err)
	}

	// Find overlapping SSTables on the next level.
	overlapping, err := t.sst.FindOverlappingFiles(dstDir, minKey, maxKey)
	if err != nil {
		return fmt.Errorf("compact L%d: find overlapping L%d: %w", level, level+1, err)
	}

	// Read all records from the selected source SSTable.
	var batches [][]model.Record
	srcRecs, err := t.sst.ReadAllRecordsFromFile(oldest)
	if err != nil {
		return fmt.Errorf("compact L%d: read %s: %w", level, oldest, err)
	}
	batches = append(batches, srcRecs)

	// Read records from overlapping destination SSTables.
	for _, f := range overlapping {
		recs, err := t.sst.ReadAllRecordsFromFile(f)
		if err != nil {
			return fmt.Errorf("compact L%d: read dst %s: %w", level, f, err)
		}
		batches = append(batches, recs)
	}

	merged := mergeAndDedup(batches)
	merged = filterExpiredRecords(merged)
	merged = filterStaleOperands(merged, deleted, epochBoundary)

	// Write merged result as chunked SSTables to the next level.
	if len(merged) > 0 {
		if err := t.sst.FlushToDirChunked(dstDir, merged, t.maxSSTableBytes()); err != nil {
			return fmt.Errorf("compact L%d: flush to L%d: %w", level, level+1, err)
		}
	}

	// Delete the selected source SSTable.
	if err := t.sst.DeleteSSTable(oldest); err != nil {
		return fmt.Errorf("compact L%d: delete src %s: %w", level, oldest, err)
	}

	// Delete old overlapping destination SSTables.
	for _, f := range overlapping {
		if err := t.sst.DeleteSSTable(f); err != nil {
			return fmt.Errorf("compact L%d: delete dst %s: %w", level, f, err)
		}
	}

	return nil
}

// compactLastLevel performs in-place compaction on the last level.
// When multiple SSTables exist at the last level, they are merged into one.
// Since there is no lower level, tombstones and expired records are purged.
func (t *LSMTree) compactLastLevel(deleted map[instanceKey]bool, epochBoundary map[instanceKey]uint64) error {
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
	merged = filterStaleOperands(merged, deleted, epochBoundary)

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
