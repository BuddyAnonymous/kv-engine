package sstable

import (
	"fmt"
	"os"
	"strings"

	"kv-engine/internal/model"
	"kv-engine/internal/probabilistic/bloom"
)

const bloomFalsePositiveRate = 0.01

func (m *Manager) writeFilterFile(filterPath string, records []model.Record) error {
	expected := len(records)
	if expected < 1 {
		expected = 1
	}

	bf := bloom.NewBloomFilter(expected, bloomFalsePositiveRate)
	for _, rec := range records {
		if rec.Key == "" {
			continue
		}
		bf.Add([]byte(rec.Key))
	}

	serialized, err := bf.Serialize()
	if err != nil {
		return err
	}

	bw := newBlockWriter(m.bm, filterPath, m.blockSize, nil)
	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		if !isFirst {
			return nil
		}
		return bw.writeBytes(m.encodeHeader(m.filterMagic))
	})
	if bw.onNewBlock != nil {
		if err := bw.onNewBlock(0, true); err != nil {
			return err
		}
	}

	// Bloom payload moze biti veci od jednog bloka, pa pisemo u komadima.
	chunkSize := bw.payloadCap()
	for off := 0; off < len(serialized); {
		end := off + chunkSize
		if end > len(serialized) {
			end = len(serialized)
		}
		if err := bw.writeBytes(serialized[off:end]); err != nil {
			return err
		}
		off = end
	}

	return bw.close()
}

func (m *Manager) maybeKeyInFilter(dataPath, key string) (bool, error) {
	basePath := strings.TrimSuffix(dataPath, ".data")
	filterPath := basePath + ".filter"

	if _, err := os.Stat(filterPath); err != nil {
		return false, err
	}

	hdr, err := m.readFileHeader(filterPath)
	if err != nil {
		return false, err
	}
	if hdr.magic != m.filterMagic {
		return false, fmt.Errorf("invalid filter magic in %s", filterPath)
	}

	payload, err := m.readAllPayload(filterPath, hdr.blockSize)
	if err != nil {
		return false, err
	}
	if len(payload) < 8 {
		return false, fmt.Errorf("filter header too short in %s", filterPath)
	}
	filterBytes := payload[8:]
	bf, err := bloom.Deserialize(filterBytes)
	if err != nil {
		return false, err
	}
	return bf.MightContain([]byte(key)), nil
}

func (m *Manager) readAllPayload(path string, blockSize int) ([]byte, error) {
	blockCount, err := m.countBlocks(path, blockSize)
	if err != nil {
		return nil, err
	}
	all := make([]byte, 0, int(blockCount)*blockSize)
	for blockNo := uint64(0); blockNo < blockCount; blockNo++ {
		payload, err := m.readPayloadBlock(path, blockSize, blockNo)
		if err != nil {
			return nil, err
		}
		all = append(all, payload...)
	}
	return all, nil
}
