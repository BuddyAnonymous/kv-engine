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
