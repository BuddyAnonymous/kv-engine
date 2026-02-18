package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"kv-engine/internal/model"
	"kv-engine/internal/sstable"
)

// instanceKey identifies a probabilistic structure instance by (Structure, Key).
type instanceKey struct {
	Structure model.StructureType
	Key       string
}

type LSMTree struct {
	cfg LSMConfig
	sst *sstable.Manager

	// baseDir is the sstable root directory
	baseDir string
}

// NewLSMTree creates a new LSM tree. It ensures all level directories exist.
func NewLSMTree(cfg LSMConfig, sst *sstable.Manager, baseDir string) (*LSMTree, error) {
	t := &LSMTree{
		cfg:     cfg,
		sst:     sst,
		baseDir: baseDir,
	}

	// Ensure level directories exist.
	for lvl := 0; lvl < cfg.MaxLevels; lvl++ {
		dir := t.levelDir(lvl)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create level %d dir: %w", lvl, err)
		}
	}

	return t, nil
}

// levelDir returns the directory path for a given level.
func (t *LSMTree) levelDir(level int) string {
	return filepath.Join(t.baseDir, fmt.Sprintf("level%d", level))
}

// Flush writes sorted records to L0 and then triggers compaction if needed.
func (t *LSMTree) Flush(records []model.Record) error {
	l0Dir := t.levelDir(0)
	if err := t.sst.FlushToDir(l0Dir, records); err != nil {
		return err
	}
	return t.maybeCompact()
}

// Get searches for a key through all levels (L0..Ln), newest first at each level.
func (t *LSMTree) Get(key string) ([]byte, bool, error) {
	rec, found, err := t.GetRecord(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return rec.Value, true, nil
}

// GetRecord searches for the latest visible KV record through all levels (L0..Ln),
// newest first at each level.
// A tombstone/expired KV on a higher level hides all older levels.
func (t *LSMTree) GetRecord(key string) (model.Record, bool, error) {
	now := uint64(time.Now().Unix())
	for lvl := 0; lvl < t.cfg.MaxLevels; lvl++ {
		dir := t.levelDir(lvl)
		rec, found, err := t.sst.GetLatestKVRecordFromDir(dir, key)
		if err != nil {
			return model.Record{}, false, err
		}
		if found {
			if rec.Tombstone || (rec.ExpiresAt > 0 && rec.ExpiresAt <= now) {
				return model.Record{}, false, nil
			}
			return rec, true, nil
		}
	}
	return model.Record{}, false, nil
}

// GetMergeOperands collects merge operands from all levels.
func (t *LSMTree) GetMergeOperands(structure model.StructureType, key string) ([]model.Record, error) {
	var allOps []model.Record
	for lvl := 0; lvl < t.cfg.MaxLevels; lvl++ {
		dir := t.levelDir(lvl)
		ops, err := t.sst.GetMergeOperandsFromDir(dir, structure, key)
		if err != nil {
			return nil, err
		}
		allOps = append(allOps, ops...)
	}

	sort.SliceStable(allOps, func(i, j int) bool {
		if allOps[i].Seq != allOps[j].Seq {
			return allOps[i].Seq < allOps[j].Seq
		}
		if allOps[i].Op != allOps[j].Op {
			return allOps[i].Op < allOps[j].Op
		}
		return bytes.Compare(allOps[i].Value, allOps[j].Value) < 0
	})

	return allOps, nil
}

// CollectAllRecords reads all records from every level and returns them in one slice.
func (t *LSMTree) CollectAllRecords() ([]model.Record, error) {
	out := make([]model.Record, 0)
	for lvl := 0; lvl < t.cfg.MaxLevels; lvl++ {
		dir := t.levelDir(lvl)
		files, err := t.sst.ListDataFilesInDir(dir)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			recs, err := t.sst.ReadAllRecordsFromFile(f)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
	}
	return out, nil
}

// maybeCompact checks all levels and triggers compaction when thresholds are exceeded.
// It also purges merge operands for deleted probabilistic instances and cleans up ProbMeta.
func (t *LSMTree) maybeCompact() error {
	// Determine which probabilistic instances have been deleted.
	deleted, err := t.getDeletedInstances()
	if err != nil {
		return fmt.Errorf("get deleted instances: %w", err)
	}

	switch t.cfg.Algorithm {
	case "size_tiered":
		err = t.sizeTieredCompaction(deleted)
	case "leveled":
		err = t.leveledCompaction(deleted)
	default:
		err = t.sizeTieredCompaction(deleted)
	}
	if err != nil {
		return err
	}

	// After compaction, clean up ProbMeta: remove stale create+delete pairs.
	if len(deleted) > 0 {
		if err := t.cleanupProbMeta(); err != nil {
			return fmt.Errorf("cleanup probmeta: %w", err)
		}
	}

	return nil
}

// getDeletedInstances reads ProbMeta and returns the set of (structure, key) pairs
// whose latest action is "delete". Merge operands for these instances should be purged.
func (t *LSMTree) getDeletedInstances() (map[instanceKey]bool, error) {
	allMeta, err := t.sst.ReadAllProbMeta()
	if err != nil {
		return nil, err
	}

	// Find the latest action for each (structure, key).
	latest := make(map[instanceKey]model.ProbMetaRecord)
	for _, rec := range allMeta {
		ik := instanceKey{rec.Structure, rec.Key}
		if existing, ok := latest[ik]; !ok || rec.Seq > existing.Seq {
			latest[ik] = rec
		}
	}

	deleted := make(map[instanceKey]bool)
	for ik, rec := range latest {
		if rec.Action == model.ProbMetaActionDelete {
			deleted[ik] = true
		}
	}
	return deleted, nil
}

// filterDeletedOperands removes merge operands that belong to deleted probabilistic instances.
func filterDeletedOperands(records []model.Record, deleted map[instanceKey]bool) []model.Record {
	if len(deleted) == 0 {
		return records
	}
	out := make([]model.Record, 0, len(records))
	for _, rec := range records {
		if rec.Kind == model.RecordKindMergeOperand {
			ik := instanceKey{rec.Structure, rec.Key}
			if deleted[ik] {
				continue
			}
		}
		out = append(out, rec)
	}
	return out
}

// cleanupProbMeta rewrites the ProbMeta file, keeping only the latest record
// per (structure, key) and only if that record is a "create" (meaning the instance
// is still alive). Deleted instances have their entries removed ONLY if no merge
// operands for that instance remain in any SSTable across all levels.
func (t *LSMTree) cleanupProbMeta() error {
	allMeta, err := t.sst.ReadAllProbMeta()
	if err != nil {
		return err
	}
	if len(allMeta) == 0 {
		return nil
	}

	// Find latest record per (structure, key).
	latest := make(map[instanceKey]model.ProbMetaRecord)
	for _, rec := range allMeta {
		ik := instanceKey{rec.Structure, rec.Key}
		if existing, ok := latest[ik]; !ok || rec.Seq > existing.Seq {
			latest[ik] = rec
		}
	}

	// For deleted instances, check if any operands still exist on any level.
	// If operands remain, we must keep the delete record so future compaction
	// continues to purge them.
	stillHasOperands := make(map[instanceKey]bool)
	for ik, rec := range latest {
		if rec.Action != model.ProbMetaActionDelete {
			continue
		}
		for lvl := 0; lvl < t.cfg.MaxLevels; lvl++ {
			dir := t.levelDir(lvl)
			ops, err := t.sst.GetMergeOperandsFromDir(dir, ik.Structure, ik.Key)
			if err != nil {
				return err
			}
			if len(ops) > 0 {
				stillHasOperands[ik] = true
				break
			}
		}
	}

	// Build the cleaned list:
	// - Active creates: always keep
	// - Deletes with remaining operands: keep (so next compaction can purge them)
	// - Deletes with no remaining operands: discard (fully cleaned up)
	var kept []model.ProbMetaRecord
	for ik, rec := range latest {
		if rec.Action == model.ProbMetaActionCreate {
			kept = append(kept, rec)
		} else if stillHasOperands[ik] {
			kept = append(kept, rec)
		}
		// else: delete with no operands remaining → discard
	}

	return t.sst.RewriteProbMeta(kept)
}

// mergeAndDedup merges multiple sorted record slices, keeping the newest version of each key.
// Records are assumed sorted by key within each slice. When multiple records share a key,
// the one with the highest Seq wins.
func mergeAndDedup(batches [][]model.Record) []model.Record {
	// Flatten all
	var all []model.Record
	for _, b := range batches {
		all = append(all, b...)
	}

	// Sort by key, then by Seq descending
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Key != all[j].Key {
			return all[i].Key < all[j].Key
		}
		return all[i].Seq > all[j].Seq
	})

	// Dedup: keep only the newest version of each key for KV records.
	// For MergeOperand records: keep all (they are additive).
	seenKV := make(map[string]bool)
	var out []model.Record
	for i := 0; i < len(all); i++ {
		rec := all[i]
		if rec.Kind == model.RecordKindMergeOperand {
			// Keep all merge operands
			out = append(out, rec)
			continue
		}
		// KV: skip if we already have a KV for this key (first occurrence has highest Seq)
		if seenKV[rec.Key] {
			continue
		}
		seenKV[rec.Key] = true
		out = append(out, rec)
	}

	// Re-sort output by key for flush
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		if out[i].Structure != out[j].Structure {
			return out[i].Structure < out[j].Structure
		}
		if out[i].Op != out[j].Op {
			return out[i].Op < out[j].Op
		}
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].Kind < out[j].Kind
	})

	return out
}

// mergeAndDedupPurge is like mergeAndDedup but also removes tombstones and expired records.
// This is safe to use only on the last level where no older versions can exist below.
func mergeAndDedupPurge(batches [][]model.Record) []model.Record {
	merged := mergeAndDedup(batches)

	now := uint64(time.Now().Unix())
	out := make([]model.Record, 0, len(merged))
	for _, rec := range merged {
		// Purge tombstones — no lower level exists.
		if rec.Kind == model.RecordKindKV && rec.Tombstone {
			continue
		}
		// Purge expired records.
		if rec.ExpiresAt > 0 && rec.ExpiresAt <= now {
			continue
		}
		out = append(out, rec)
	}
	return out
}
