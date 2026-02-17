package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	tocVersion    uint64 = 1
	tocModeMulti  uint64 = 1
	tocModeSingle uint64 = 2
)

func (m *Manager) writeTOC(basePath string, mode uint64) error {
	if mode != tocModeMulti && mode != tocModeSingle {
		return fmt.Errorf("invalid toc mode: %d", mode)
	}
	tocPath := basePath + ".toc"
	_ = os.Remove(tocPath)

	bw := newBlockWriter(m.bm, tocPath, m.blockSize, nil)
	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		if !isFirst {
			return nil
		}
		return bw.writeBytes(m.encodeHeader(m.tocMagic))
	})
	if bw.onNewBlock != nil {
		if err := bw.onNewBlock(0, true); err != nil {
			return err
		}
	}
	if err := bw.writeBytes(uvarintBytes(tocVersion)); err != nil {
		return err
	}
	if err := bw.writeBytes(uvarintBytes(mode)); err != nil {
		return err
	}
	return bw.close()
}

func (m *Manager) readTOCMode(tocPath string) (uint64, error) {
	hdr, err := m.readFileHeader(tocPath)
	if err != nil {
		return 0, err
	}
	if hdr.magic != m.tocMagic {
		return 0, fmt.Errorf("invalid toc magic in %s", tocPath)
	}

	payload, err := m.readAllPayload(tocPath, hdr.blockSize)
	if err != nil {
		return 0, err
	}
	if len(payload) < 8 {
		return 0, fmt.Errorf("toc header too short in %s", tocPath)
	}
	if string(payload[:4]) != string(m.tocMagic[:]) {
		return 0, fmt.Errorf("invalid toc payload magic in %s", tocPath)
	}

	off := 8
	ver, err := readUvarintAt(payload, &off)
	if err != nil {
		return 0, err
	}
	if ver != tocVersion {
		return 0, fmt.Errorf("unsupported toc version in %s: %d", tocPath, ver)
	}
	mode, err := readUvarintAt(payload, &off)
	if err != nil {
		return 0, err
	}
	if mode != tocModeMulti && mode != tocModeSingle {
		return 0, fmt.Errorf("invalid toc mode in %s: %d", tocPath, mode)
	}
	if off != len(payload) {
		return 0, fmt.Errorf("toc trailing bytes in %s", tocPath)
	}
	return mode, nil
}

func parseSSTTimestampFromBase(baseName string) int64 {
	trimmed := strings.TrimPrefix(baseName, "sst_")
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

type tableRef struct {
	basePath string
	baseName string
	ts       int64
	mode     uint64
}

func (m *Manager) listAllTableRefsNewestFirst() ([]tableRef, error) {
	pattern := filepath.Join(m.dir, "sst_*.toc")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	refs := make([]tableRef, 0, len(matches))
	for _, tocPath := range matches {
		tocMode, err := m.readTOCMode(tocPath)
		if err != nil {
			return nil, err
		}
		if tocMode != tocModeMulti && tocMode != tocModeSingle {
			continue
		}

		basePath := strings.TrimSuffix(tocPath, ".toc")
		baseName := filepath.Base(basePath)
		switch tocMode {
		case tocModeMulti:
			if _, err := os.Stat(basePath + ".data"); err != nil {
				continue
			}
			if _, err := os.Stat(basePath + ".index"); err != nil {
				continue
			}
			if _, err := os.Stat(basePath + ".summary"); err != nil {
				continue
			}
		case tocModeSingle:
			if _, err := os.Stat(basePath + ".sst"); err != nil {
				continue
			}
		}

		refs = append(refs, tableRef{
			basePath: basePath,
			baseName: baseName,
			ts:       parseSSTTimestampFromBase(baseName),
			mode:     tocMode,
		})
	}

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ts != refs[j].ts {
			return refs[i].ts > refs[j].ts
		}
		return refs[i].baseName > refs[j].baseName
	})
	return refs, nil
}
