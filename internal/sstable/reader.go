package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"kv-engine/internal/model"
)

type summaryMeta struct {
	stride  uint64
	minKey  string
	maxKey  string
	entries []summaryEntry
}

type summaryEntry struct {
	key          string
	indexBlockNo uint64
}

type indexEntry struct {
	key         string
	dataBlockNo uint64
}

type fileHeader struct {
	magic     [4]byte
	blockSize int
	flags     uint16
}

func (m *Manager) Get(key string) ([]byte, bool, error) {
	if !m.multiFileSSTable {
		return nil, false, fmt.Errorf("single-file sstable get is not implemented")
	}

	dataFiles, err := m.listDataFilesNewestFirst()
	if err != nil {
		return nil, false, err
	}
	if len(dataFiles) == 0 {
		return nil, false, nil
	}

	now := uint64(time.Now().Unix())
	for _, dataPath := range dataFiles {
		rec, found, err := m.getFromDataFile(dataPath, key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		if rec.Tombstone || isExpired(rec, now) {
			return nil, false, nil
		}
		return rec.Value, true, nil
	}

	return nil, false, nil
}

func (m *Manager) getFromDataFile(dataPath, key string) (model.Record, bool, error) {
	basePath := strings.TrimSuffix(dataPath, ".data")
	indexPath := basePath + ".index"
	summaryPath := basePath + ".summary"

	dataHdr, err := m.readFileHeader(dataPath)
	if err != nil {
		return model.Record{}, false, err
	}
	if dataHdr.magic != m.dataMagic {
		return model.Record{}, false, fmt.Errorf("invalid data magic in %s", dataPath)
	}

	indexHdr, err := m.readFileHeader(indexPath)
	if err != nil {
		return model.Record{}, false, err
	}
	if indexHdr.magic != m.indexMagic {
		return model.Record{}, false, fmt.Errorf("invalid index magic in %s", indexPath)
	}

	summaryHdr, err := m.readFileHeader(summaryPath)
	if err != nil {
		return model.Record{}, false, err
	}
	if summaryHdr.magic != m.summMagic {
		return model.Record{}, false, fmt.Errorf("invalid summary magic in %s", summaryPath)
	}

	summ, err := m.readSummaryMeta(summaryPath, summaryHdr.blockSize, key)
	if err != nil {
		return model.Record{}, false, err
	}
	if summ.minKey != "" && key < summ.minKey {
		return model.Record{}, false, nil
	}
	if summ.maxKey != "" && key > summ.maxKey {
		return model.Record{}, false, nil
	}

	startIndexBlock := uint64(0)
	if len(summ.entries) > 0 {
		startIndexBlock = summ.entries[len(summ.entries)-1].indexBlockNo
	}

	indexEntries, err := m.readIndexEntriesFromBlock(indexPath, indexHdr.blockSize, startIndexBlock, key)
	if err != nil {
		return model.Record{}, false, err
	}
	if len(indexEntries) == 0 {
		return model.Record{}, false, nil
	}

	startDataBlock := indexEntries[len(indexEntries)-1].dataBlockNo

	endDataBlock, err := m.countBlocks(dataPath, dataHdr.blockSize)
	if err != nil {
		return model.Record{}, false, err
	}
	if startDataBlock >= endDataBlock {
		return model.Record{}, false, fmt.Errorf("invalid data block range [%d,%d) for %s", startDataBlock, endDataBlock, dataPath)
	}

	return m.searchDataRange(dataPath, dataHdr.blockSize, key, startDataBlock, endDataBlock)
}

func (m *Manager) searchDataRange(dataPath string, blockSize int, key string, startBlock, endBlock uint64) (model.Record, bool, error) {
	prevKey := ""
	var pending []byte

	for blockNo := startBlock; blockNo < endBlock; blockNo++ {
		payload, err := m.readPayloadBlock(dataPath, blockSize, blockNo)
		if err != nil {
			return model.Record{}, false, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return model.Record{}, false, fmt.Errorf("data header too short in %s", dataPath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return model.Record{}, false, fmt.Errorf("invalid data magic in %s", dataPath)
			}
			off = 8
		}

		for off < len(payload) {
			flags := payload[off]
			fragType := flags & 0b11

			if len(pending) == 0 {
				switch fragType {
				case 0b00:
					rec, consumed, err := decodeDataRecord(payload[off:], prevKey)
					if err != nil {
						return model.Record{}, false, err
					}
					off += consumed
					prevKey = rec.Key

					if rec.Key == key {
						return rec, true, nil
					}
					if rec.Key > key {
						return model.Record{}, false, nil
					}

				case 0b10:
					// FIRST fragment: by format, this is the rest of payload bytes in the current block.
					pending = append(pending, payload[off:]...)
					off = len(payload)

				case 0b01, 0b11:
					return model.Record{}, false, fmt.Errorf("unexpected continuation fragment in %s block %d", dataPath, blockNo)

				default:
					return model.Record{}, false, fmt.Errorf("unknown fragment flag %d in %s block %d", fragType, dataPath, blockNo)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return model.Record{}, false, fmt.Errorf("expected continuation fragment in %s block %d", dataPath, blockNo)
			}

			off++ // flags
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return model.Record{}, false, err
			}
			if off+int(chunkLen) > len(payload) {
				return model.Record{}, false, fmt.Errorf("fragment chunk out of bounds in %s block %d", dataPath, blockNo)
			}

			pending = append(pending, payload[off:off+int(chunkLen)]...)
			off += int(chunkLen)

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return model.Record{}, false, err
				}
				if consumed != len(pending) {
					return model.Record{}, false, fmt.Errorf("fragment assembly has extra bytes in %s block %d", dataPath, blockNo)
				}
				pending = pending[:0]
				prevKey = rec.Key

				if rec.Key == key {
					return rec, true, nil
				}
				if rec.Key > key {
					return model.Record{}, false, nil
				}
			}
		}
	}

	if len(pending) != 0 {
		return model.Record{}, false, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return model.Record{}, false, nil
}

func decodeDataRecord(buf []byte, prevKey string) (model.Record, int, error) {
	off := 0
	if len(buf) == 0 {
		return model.Record{}, 0, fmt.Errorf("empty data record buffer")
	}

	flags := buf[off]
	off++

	expiresAt, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	shared, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	suffixLen, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}

	if int(shared) > len(prevKey) {
		return model.Record{}, 0, fmt.Errorf("invalid shared prefix: %d > %d", shared, len(prevKey))
	}
	if off+int(suffixLen) > len(buf) {
		return model.Record{}, 0, fmt.Errorf("suffix out of bounds")
	}

	suffix := string(buf[off : off+int(suffixLen)])
	off += int(suffixLen)
	key := prevKey[:int(shared)] + suffix

	seq, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	valLen, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	if off+int(valLen) > len(buf) {
		return model.Record{}, 0, fmt.Errorf("value out of bounds")
	}

	val := make([]byte, int(valLen))
	copy(val, buf[off:off+int(valLen)])
	off += int(valLen)

	tombstone := (flags & 0b00000100) != 0
	if tombstone {
		val = nil
	}

	rec := model.Record{
		Key:       key,
		Value:     val,
		Tombstone: tombstone,
		Seq:       seq,
		ExpiresAt: expiresAt,
	}
	return rec, off, nil
}

func (m *Manager) readSummaryMeta(path string, blockSize int, key string) (summaryMeta, error) {
	blockCount, err := m.countBlocks(path, blockSize)
	if err != nil {
		return summaryMeta{}, err
	}
	if blockCount == 0 {
		return summaryMeta{}, fmt.Errorf("summary file is empty: %s", path)
	}

	payload0, err := m.readPayloadBlock(path, blockSize, 0)
	if err != nil {
		return summaryMeta{}, err
	}
	if len(payload0) < 8 {
		return summaryMeta{}, fmt.Errorf("summary header too short: %s", path)
	}
	if string(payload0[:4]) != string(m.summMagic[:]) {
		return summaryMeta{}, fmt.Errorf("invalid summary magic in %s", path)
	}

	off := 8
	stride, err := readUvarintAt(payload0, &off)
	if err != nil {
		return summaryMeta{}, err
	}
	minLen, err := readUvarintAt(payload0, &off)
	if err != nil {
		return summaryMeta{}, err
	}
	if off+int(minLen) > len(payload0) {
		return summaryMeta{}, fmt.Errorf("summary minKey out of bounds in %s", path)
	}
	minKey := string(payload0[off : off+int(minLen)])
	off += int(minLen)

	maxLen, err := readUvarintAt(payload0, &off)
	if err != nil {
		return summaryMeta{}, err
	}
	if off+int(maxLen) > len(payload0) {
		return summaryMeta{}, fmt.Errorf("summary maxKey out of bounds in %s", path)
	}
	maxKey := string(payload0[off : off+int(maxLen)])
	off += int(maxLen)

	// ako je trazeni key van [min,max], nema potrebe da citamo summary entries.
	if key != "" {
		if (minKey != "" && key < minKey) || (maxKey != "" && key > maxKey) {
			return summaryMeta{
				stride:  stride,
				minKey:  minKey,
				maxKey:  maxKey,
				entries: nil,
			}, nil
		}
	}

	entries, err := decodeSummaryEntries(payload0, off, key)
	if err != nil {
		return summaryMeta{}, err
	}

	for i := uint64(1); i < blockCount; i++ {
		payload, err := m.readPayloadBlock(path, blockSize, i)
		if err != nil {
			return summaryMeta{}, err
		}
		es, err := decodeSummaryEntries(payload, 0, key)
		if err != nil {
			return summaryMeta{}, err
		}
		entries = append(entries, es...)
	}

	return summaryMeta{
		stride:  stride,
		minKey:  minKey,
		maxKey:  maxKey,
		entries: entries,
	}, nil
}

func decodeSummaryEntries(payload []byte, start int, targetKey string) ([]summaryEntry, error) {
	off := start
	prevKey := ""
	out := make([]summaryEntry, 0)

	for off < len(payload) {
		shared, err := readUvarintAt(payload, &off)
		if err != nil {
			return nil, err
		}
		suffixLen, err := readUvarintAt(payload, &off)
		if err != nil {
			return nil, err
		}
		if int(shared) > len(prevKey) {
			return nil, fmt.Errorf("summary shared prefix out of range")
		}
		if off+int(suffixLen) > len(payload) {
			return nil, fmt.Errorf("summary suffix out of bounds")
		}

		suffix := string(payload[off : off+int(suffixLen)])
		off += int(suffixLen)
		key := prevKey[:int(shared)] + suffix

		if targetKey != "" && key > targetKey {
			break
		}

		indexBlockNo, err := readUvarintAt(payload, &off)
		if err != nil {
			return nil, err
		}

		out = append(out, summaryEntry{key: key, indexBlockNo: indexBlockNo})
		prevKey = key
	}
	return out, nil
}

func (m *Manager) readIndexEntriesFromBlock(path string, blockSize int, startBlock uint64, targetKey string) ([]indexEntry, error) {
	blockCount, err := m.countBlocks(path, blockSize)
	if err != nil {
		return nil, err
	}
	if startBlock >= blockCount {
		return nil, nil
	}

	out := make([]indexEntry, 0)
	for blockNo := startBlock; blockNo < blockCount; blockNo++ {
		payload, err := m.readPayloadBlock(path, blockSize, blockNo)
		if err != nil {
			return nil, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return nil, fmt.Errorf("index header too short: %s", path)
			}
			if string(payload[:4]) != string(m.indexMagic[:]) {
				return nil, fmt.Errorf("invalid index magic in %s", path)
			}
			off = 8
		}

		prevKey := ""
		for off < len(payload) {
			shared, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}
			suffixLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}
			if int(shared) > len(prevKey) {
				return nil, fmt.Errorf("index shared prefix out of range")
			}
			if off+int(suffixLen) > len(payload) {
				return nil, fmt.Errorf("index suffix out of bounds")
			}

			suffix := string(payload[off : off+int(suffixLen)])
			off += int(suffixLen)
			key := prevKey[:int(shared)] + suffix

			dataBlockNo, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}

			// Index je sortiran; nakon prvog key > target nema potrebe za daljim citanjem.
			if targetKey != "" && key > targetKey {
				return out, nil
			}

			out = append(out, indexEntry{
				key:         key,
				dataBlockNo: dataBlockNo,
			})
			prevKey = key
		}
	}

	return out, nil
}

func (m *Manager) readPayloadBlock(path string, blockSize int, blockNo uint64) ([]byte, error) {
	blockData, err := m.bm.ReadBlock(path, blockNo, blockSize)
	if err != nil {
		return nil, err
	}

	maxPayload := blockSize - crcBytes - payloadLenBytes
	payloadLen := int(binary.LittleEndian.Uint32(blockData[0:payloadLenBytes]))
	if payloadLen < 0 || payloadLen > maxPayload {
		return nil, fmt.Errorf("invalid payload length at %s block %d: %d", path, blockNo, payloadLen)
	}

	crcWant := binary.LittleEndian.Uint32(blockData[blockSize-crcBytes : blockSize])
	crcGot := crc32.ChecksumIEEE(blockData[:blockSize-crcBytes])
	if crcGot != crcWant {
		return nil, fmt.Errorf("crc mismatch at %s block %d", path, blockNo)
	}

	payload := make([]byte, payloadLen)
	copy(payload, blockData[payloadLenBytes:payloadLenBytes+payloadLen])
	return payload, nil
}

func (m *Manager) countBlocks(path string, blockSize int) (uint64, error) {
	if blockSize <= 0 {
		return 0, fmt.Errorf("invalid block size %d for %s", blockSize, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size == 0 {
		return 0, nil
	}
	if size%int64(blockSize) != 0 {
		return 0, fmt.Errorf("unaligned file size for %s: %d", path, size)
	}
	return uint64(size / int64(blockSize)), nil
}

func (m *Manager) readFileHeader(path string) (fileHeader, error) {
	// Layout: [payloadLen(4B)] [magic(4B)|blockSize(u16)|flags(u16)] ...
	raw, err := m.bm.ReadAt(path, int64(payloadLenBytes), 8)
	if err != nil {
		return fileHeader{}, err
	}

	var magic [4]byte
	copy(magic[:], raw[:4])
	blockSize := int(binary.LittleEndian.Uint16(raw[4:6]))
	flags := binary.LittleEndian.Uint16(raw[6:8])

	if blockSize <= crcBytes+payloadLenBytes {
		return fileHeader{}, fmt.Errorf("invalid header block size %d in %s", blockSize, path)
	}

	return fileHeader{
		magic:     magic,
		blockSize: blockSize,
		flags:     flags,
	}, nil
}

func readUvarintAt(b []byte, off *int) (uint64, error) {
	if *off >= len(b) {
		return 0, fmt.Errorf("uvarint offset out of range")
	}
	v, n := binary.Uvarint(b[*off:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint encoding")
	}
	*off += n
	return v, nil
}

func (m *Manager) listDataFilesNewestFirst() ([]string, error) {
	pattern := filepath.Join(m.dir, "sst_*.data")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	type candidate struct {
		path string
		ts   int64
		name string
	}
	files := make([]candidate, 0, len(matches))
	for _, dataPath := range matches {
		base := strings.TrimSuffix(dataPath, ".data")
		if _, err := os.Stat(base + ".index"); err != nil {
			continue
		}
		if _, err := os.Stat(base + ".summary"); err != nil {
			continue
		}

		name := filepath.Base(dataPath)
		files = append(files, candidate{
			path: dataPath,
			ts:   parseSSTTimestamp(name),
			name: name,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].ts != files[j].ts {
			return files[i].ts > files[j].ts
		}
		return files[i].name > files[j].name
	})

	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.path)
	}
	return out, nil
}

func parseSSTTimestamp(fileName string) int64 {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(fileName, "sst_"), ".data")
	v, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func isExpired(rec model.Record, now uint64) bool {
	return rec.ExpiresAt > 0 && rec.ExpiresAt <= now
}
