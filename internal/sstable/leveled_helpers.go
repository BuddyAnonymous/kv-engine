package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"kv-engine/internal/model"
)

// GetKeyRangeForFile returns the min and max key for an SSTable identified by base path.
// Reads only the summary header block — very lightweight.
func (m *Manager) GetKeyRangeForFile(basePath string) (string, string, error) {
	refs, err := m.listTableRefsInDir(filepath.Dir(basePath))
	if err != nil {
		return "", "", err
	}
	base := filepath.Base(basePath)
	for _, ref := range refs {
		if ref.baseName == base {
			return m.getTableKeyRange(ref)
		}
	}
	return "", "", fmt.Errorf("table not found: %s", basePath)
}

// FindOldestFile returns the base path of the oldest SSTable in the directory
// (smallest timestamp in filename). Returns "" if no SSTables exist.
func (m *Manager) FindOldestFile(dir string) (string, error) {
	refs, err := m.listTableRefsInDir(dir)
	if err != nil {
		return "", err
	}
	if len(refs) == 0 {
		return "", nil
	}
	// listTableRefsInDir returns newest-first, so oldest is last.
	oldest := refs[len(refs)-1]
	return oldest.basePath, nil
}

// FindOverlappingFiles returns base paths of all SSTables in the directory
// whose key range overlaps with [minKey, maxKey].
func (m *Manager) FindOverlappingFiles(dir, minKey, maxKey string) ([]string, error) {
	refs, err := m.listTableRefsInDir(dir)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, ref := range refs {
		tblMin, tblMax, err := m.getTableKeyRange(ref)
		if err != nil {
			// If we can't read range, assume overlap to be safe.
			result = append(result, ref.basePath)
			continue
		}
		// Check overlap: two ranges [minKey,maxKey] and [tblMin,tblMax] overlap if
		// minKey <= tblMax AND maxKey >= tblMin
		if tblMax != "" && minKey > tblMax {
			continue
		}
		if tblMin != "" && maxKey < tblMin {
			continue
		}
		result = append(result, ref.basePath)
	}
	return result, nil
}

// FlushToDirChunked writes sorted records as multiple SSTables into the specified directory,
// each with approximately maxBytes of record data. If maxBytes <= 0, all records are written
// as a single SSTable.
func (m *Manager) FlushToDirChunked(dir string, records []model.Record, maxBytes int64) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	if maxBytes <= 0 || len(records) == 0 {
		// Fallback: write as single SSTable.
		return m.FlushToDir(dir, records)
	}

	var chunk []model.Record
	var chunkSize int64

	for _, rec := range records {
		recSize := estimateRecordSize(&rec)
		if chunkSize+recSize > maxBytes && len(chunk) > 0 {
			if err := m.flushChunk(dir, chunk); err != nil {
				return err
			}
			chunk = nil
			chunkSize = 0
		}
		chunk = append(chunk, rec)
		chunkSize += recSize
	}

	if len(chunk) > 0 {
		if err := m.flushChunk(dir, chunk); err != nil {
			return err
		}
	}

	return nil
}

// flushChunk writes a single SSTable to the directory with a unique timestamp-based name.
func (m *Manager) flushChunk(dir string, records []model.Record) error {
	base := fmt.Sprintf("sst_%d", time.Now().UnixNano())
	basePath := filepath.Join(dir, base)

	if m.multiFileSSTable {
		return m.WriteMultiFile(basePath, records)
	}
	return m.WriteSingleFile(basePath, records)
}

// estimateRecordSize returns an approximate size in bytes for a single record.
func estimateRecordSize(r *model.Record) int64 {
	size := 0
	size += len(r.Key)
	size += len(r.Value)
	size += 1  // Tombstone
	size += 8  // Seq
	size += 8  // ExpiresAt
	size += 1  // Kind
	size += 1  // Structure
	size += 1  // Op
	size += 32 // overhead
	return int64(size)
}
