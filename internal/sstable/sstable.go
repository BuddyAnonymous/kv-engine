package sstable

import (
	"fmt"
	"kv-engine/internal/block"
	"kv-engine/internal/model"
	"os"
	"path/filepath"
	"time"
)

type Manager struct {
	dir              string
	multiFileSSTable bool

	bm        *block.BlockManager
	blockSize int

	flags         uint16
	summaryStride uint64
	dataMagic     [4]byte
	indexMagic    [4]byte
	summMagic     [4]byte
	filterMagic   [4]byte
	merkleMagic   [4]byte
}

func New(dir string, multiFileSSTable bool, bm *block.BlockManager, blockSize int, summaryStride uint64) *Manager {
	return &Manager{
		dir:              dir,
		multiFileSSTable: multiFileSSTable,
		bm:               bm,
		blockSize:        blockSize,
		summaryStride:    summaryStride,
		dataMagic:        [4]byte{'D', 'A', 'T', 'A'},
		indexMagic:       [4]byte{'I', 'N', 'D', 'X'},
		summMagic:        [4]byte{'S', 'U', 'M', 'M'},
		filterMagic:      [4]byte{'F', 'I', 'L', 'T'},
		merkleMagic:      [4]byte{'M', 'R', 'K', 'L'},
	}
}

func (m *Manager) Flush(records []model.Record) error {

	if err := os.MkdirAll(m.dir, 0755); err != nil {
		return err
	}

	base := fmt.Sprintf("sst_%d", time.Now().UnixNano())
	basePath := filepath.Join(m.dir, base)

	if m.multiFileSSTable {
		return m.WriteMultiFile(basePath, records)
	} else {
		return m.WriteSingleFile(basePath, records)
	}
}

// FlushToDir writes sorted records as a new SSTable into the specified directory.
func (m *Manager) FlushToDir(dir string, records []model.Record) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	base := fmt.Sprintf("sst_%d", time.Now().UnixNano())
	basePath := filepath.Join(dir, base)

	if m.multiFileSSTable {
		return m.WriteMultiFile(basePath, records)
	} else {
		return m.WriteSingleFile(basePath, records)
	}
}

// DeleteSSTable removes all files belonging to a single SSTable (data, index, summary, filter, merkle).
func (m *Manager) DeleteSSTable(dataPath string) error {
	exts := []string{".data", ".index", ".summary", ".filter", ".merkle"}
	basePath := dataPath
	for _, ext := range exts {
		if len(dataPath) > len(ext) && dataPath[len(dataPath)-len(ext):] == ext {
			basePath = dataPath[:len(dataPath)-len(ext)]
			break
		}
	}

	for _, ext := range exts {
		p := basePath + ext
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove %s: %w", p, err)
		}
	}
	return nil
}

// Dir returns the directory this manager writes to.
func (m *Manager) Dir() string {
	return m.dir
}

// BlockManagerRef returns the underlying block manager.
func (m *Manager) BlockManagerRef() *block.BlockManager {
	return m.bm
}

// BlockSize returns the configured block size.
func (m *Manager) BlockSizeVal() int {
	return m.blockSize
}

// SummaryStrideVal returns the configured summary stride.
func (m *Manager) SummaryStrideVal() uint64 {
	return m.summaryStride
}

// MultiFile returns whether this manager uses multi-file SSTables.
func (m *Manager) MultiFile() bool {
	return m.multiFileSSTable
}

var _ ManagerIface = (*Manager)(nil)
