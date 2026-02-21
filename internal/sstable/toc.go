package sstable

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	tocVersion    uint64 = 1
	tocModeMulti  uint64 = 1
	tocModeSingle uint64 = 2
)
// Writes TOC file with given mode (multi/single)
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
// Returns TOC mode
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
// Return timestamp from file name
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
	return m.listTableRefsInDir(m.dir)
}
