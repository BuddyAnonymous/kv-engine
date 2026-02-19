package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
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

	tables, err := m.listAllTableRefsNewestFirst()
	if err != nil {
		return nil, false, err
	}
	if len(tables) == 0 {
		return nil, false, nil
	}

	now := uint64(time.Now().Unix())
	for _, tbl := range tables {
		switch tbl.mode {
		case tocModeSingle:
			singlePath := tbl.basePath + ".sst"
			footer, blockSize, err := m.readSingleFooter(singlePath)
			if err != nil {
				return nil, false, err
			}
			maybeInFilter, err := m.maybeKeyInSingleFilter(singlePath, footer, blockSize, key)
			if err != nil {
				return nil, false, err
			}
			if !maybeInFilter {
				continue
			}
			rec, found, err := m.getLatestKVFromSingleFile(singlePath, footer, blockSize, key)
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

		case tocModeMulti:
			dataPath := tbl.basePath + ".data"
			maybeInFilter, err := m.maybeKeyInFilter(dataPath, key)
			if err != nil {
				return nil, false, err
			}
			if !maybeInFilter {
				continue
			}

			rec, found, err := m.getLatestKVFromDataFile(dataPath, key)
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
	}

	return nil, false, nil
}

func (m *Manager) GetMergeOperands(structure model.StructureType, key string) ([]model.Record, error) {

	if structure == model.StructureTypeNone {
		return nil, fmt.Errorf("invalid merge structure type")
	}

	tables, err := m.listAllTableRefsNewestFirst()
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}

	now := uint64(time.Now().Unix())
	var ops []model.Record

	for _, tbl := range tables {
		switch tbl.mode {
		case tocModeSingle:
			singlePath := tbl.basePath + ".sst"
			footer, blockSize, err := m.readSingleFooter(singlePath)
			if err != nil {
				return nil, err
			}
			err = m.scanSingleDataSection(singlePath, blockSize, footer, func(rec model.Record) (bool, error) {

				if rec.Key < key {
					return false, nil
				}
				if rec.Key > key {
					return true, nil
				}
				if rec.Kind != model.RecordKindMergeOperand {
					return false, nil
				}
				if rec.Structure != structure {
					return false, nil
				}
				if rec.Op != model.MergeOpAdd {
					return false, nil
				}
				if isExpired(rec, now) {
					return false, nil
				}
				ops = append(ops, rec)
				return false, nil
			})
			if err != nil {
				return nil, err
			}

		case tocModeMulti:
			dataPath := tbl.basePath + ".data"
			recs, found, err := m.getKeyRecordsFromDataFile(dataPath, key)
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			for _, rec := range recs {
				if rec.Kind != model.RecordKindMergeOperand {
					continue
				}
				if rec.Structure != structure {
					continue
				}
				if rec.Op != model.MergeOpAdd {
					continue
				}
				if isExpired(rec, now) {
					continue
				}
				ops = append(ops, rec)
			}
		}
	}

	sort.SliceStable(ops, func(i, j int) bool {

		if ops[i].Seq != ops[j].Seq {
			return ops[i].Seq < ops[j].Seq
		}
		if ops[i].Op != ops[j].Op {
			return ops[i].Op < ops[j].Op
		}
		return bytes.Compare(ops[i].Value, ops[j].Value) < 0
	})

	return ops, nil
}

func (m *Manager) getLatestKVFromDataFile(dataPath, key string) (model.Record, bool, error) {

	blockSize, startDataBlock, endDataBlock, ok, err := m.locateDataRangeForAllKeyRecords(dataPath, key)
	if err != nil {
		return model.Record{}, false, err
	}
	if !ok {
		return model.Record{}, false, nil
	}
	return m.searchDataRangeForLatestKV(dataPath, blockSize, key, startDataBlock, endDataBlock)
}

func (m *Manager) getKeyRecordsFromDataFile(dataPath, key string) ([]model.Record, bool, error) {

	blockSize, startDataBlock, endDataBlock, ok, err := m.locateDataRangeForAllKeyRecords(dataPath, key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return m.searchDataRangeForKey(dataPath, blockSize, key, startDataBlock, endDataBlock)
}

func (m *Manager) locateDataRangeForAllKeyRecords(dataPath, key string) (blockSize int, startDataBlock, endDataBlock uint64, ok bool, err error) {

	basePath := strings.TrimSuffix(dataPath, ".data")
	indexPath := basePath + ".index"
	summaryPath := basePath + ".summary"

	dataHdr, err := m.readFileHeader(dataPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if dataHdr.magic != m.dataMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid data magic in %s", dataPath)
	}

	indexHdr, err := m.readFileHeader(indexPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if indexHdr.magic != m.indexMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid index magic in %s", indexPath)
	}

	summaryHdr, err := m.readFileHeader(summaryPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if summaryHdr.magic != m.summMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid summary magic in %s", summaryPath)
	}

	summ, err := m.readSummaryMeta(summaryPath, summaryHdr.blockSize, key)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if summ.minKey != "" && key < summ.minKey {
		return 0, 0, 0, false, nil
	}
	if summ.maxKey != "" && key > summ.maxKey {
		return 0, 0, 0, false, nil
	}

	// Use summary to choose where to start reading index blocks.
	// For correctness with duplicate keys across blocks, we anchor to the last summary
	// key that is strictly smaller than target key.
	startIndexBlock := uint64(0)
	for _, se := range summ.entries {
		if se.key < key {
			startIndexBlock = se.indexBlockNo
			continue
		}
		break
	}

	indexEntries, err := m.readIndexEntriesFromBlock(indexPath, indexHdr.blockSize, startIndexBlock, key)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if len(indexEntries) == 0 {
		return 0, 0, 0, false, nil
	}

	var (
		hasPrev   bool
		predBlock uint64
	)
	for _, ie := range indexEntries {
		if ie.key < key {
			hasPrev = true
			predBlock = ie.dataBlockNo
			continue
		}
		break
	}

	if hasPrev {
		startDataBlock = predBlock
	} else {
		startDataBlock = 0
	}

	endDataBlock, err = m.countBlocks(dataPath, dataHdr.blockSize)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if startDataBlock >= endDataBlock {
		return 0, 0, 0, false, fmt.Errorf("invalid data block range [%d,%d) for %s", startDataBlock, endDataBlock, dataPath)
	}

	return dataHdr.blockSize, startDataBlock, endDataBlock, true, nil
}

func (m *Manager) searchDataRangeForKey(dataPath string, blockSize int, key string, startBlock, endBlock uint64) ([]model.Record, bool, error) {

	prevKey := ""
	var pending []byte
	out := make([]model.Record, 0, 1)

	for blockNo := startBlock; blockNo < endBlock; blockNo++ {
		payload, err := m.readPayloadBlock(dataPath, blockSize, blockNo)
		if err != nil {
			return nil, false, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return nil, false, fmt.Errorf("data header too short in %s", dataPath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return nil, false, fmt.Errorf("invalid data magic in %s", dataPath)
			}
			off = 8
		}

		for off < len(payload) {
			flags := payload[off]
			fragType := flags & fragTypeMask

			if len(pending) == 0 {
				switch fragType {
				case 0b00:
					rec, consumed, err := decodeDataRecord(payload[off:], prevKey)
					if err != nil {
						return nil, false, err
					}
					off += consumed
					prevKey = rec.Key

					if rec.Key == key {
						out = append(out, rec)
						continue
					}
					if rec.Key > key {
						return out, len(out) > 0, nil
					}

				case 0b10:
					// FIRST fragment: by format, this is the rest of payload bytes in the current block.
					pending = append(pending, payload[off:]...)
					off = len(payload)

				case 0b01, 0b11:
					return nil, false, fmt.Errorf("unexpected continuation fragment in %s block %d", dataPath, blockNo)

				default:
					return nil, false, fmt.Errorf("unknown fragment flag %d in %s block %d", fragType, dataPath, blockNo)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return nil, false, fmt.Errorf("expected continuation fragment in %s block %d", dataPath, blockNo)
			}

			off++ // flags
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, false, err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return nil, false, fmt.Errorf("fragment chunk out of bounds in %s block %d", dataPath, blockNo)
			}

			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return nil, false, err
				}
				if consumed != len(pending) {
					return nil, false, fmt.Errorf("fragment assembly has extra bytes in %s block %d", dataPath, blockNo)
				}
				pending = pending[:0]
				prevKey = rec.Key

				if rec.Key == key {
					out = append(out, rec)
					continue
				}
				if rec.Key > key {
					return out, len(out) > 0, nil
				}
			}
		}
	}

	if len(pending) != 0 {
		return nil, false, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return out, len(out) > 0, nil
}

func (m *Manager) searchDataRangeForLatestKV(dataPath string, blockSize int, key string, startBlock, endBlock uint64) (model.Record, bool, error) {

	prevKey := ""
	var pending []byte
	var (
		best  model.Record
		found bool
	)

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
			fragType := flags & fragTypeMask

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
						if rec.Kind == model.RecordKindKV && (!found || rec.Seq > best.Seq) {
							best = rec
							found = true
						}
						continue
					}
					if rec.Key > key {
						return best, found, nil
					}

				case 0b10:
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

			off++
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return model.Record{}, false, err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return model.Record{}, false, fmt.Errorf("fragment chunk out of bounds in %s block %d", dataPath, blockNo)
			}
			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

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
					if rec.Kind == model.RecordKindKV && (!found || rec.Seq > best.Seq) {
						best = rec
						found = true
					}
					continue
				}
				if rec.Key > key {
					return best, found, nil
				}
			}
		}
	}

	if len(pending) != 0 {
		return model.Record{}, false, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return best, found, nil
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

	if shared > uint64(len(prevKey)) {
		return model.Record{}, 0, fmt.Errorf("invalid shared prefix: %d > %d", shared, len(prevKey))
	}
	sharedI := int(shared)
	suffixLenI, err := checkedChunkLen(suffixLen, len(buf)-off, "suffix")
	if err != nil {
		return model.Record{}, 0, fmt.Errorf("suffix out of bounds")
	}

	suffix := string(buf[off : off+suffixLenI])
	off += suffixLenI
	key := prevKey[:sharedI] + suffix

	seq, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	valLen, err := readUvarintAt(buf, &off)
	if err != nil {
		return model.Record{}, 0, err
	}
	valLenI, err := checkedChunkLen(valLen, len(buf)-off, "value")
	if err != nil {
		return model.Record{}, 0, fmt.Errorf("value out of bounds")
	}

	val := make([]byte, valLenI)
	copy(val, buf[off:off+valLenI])
	off += valLenI

	tombstone := (flags & tombstoneMask) != 0
	kind := model.RecordKindKV
	if (flags & kindMask) != 0 {
		kind = model.RecordKindMergeOperand
	}
	structure := model.StructureType((flags & structureMask) >> structureShift)
	op := model.MergeOpType((flags & opMask) >> opShift)
	if tombstone {
		val = nil
	}
	if kind == model.RecordKindKV {
		structure = model.StructureTypeNone
		op = model.MergeOpNone
	}

	rec := model.Record{
		Key:       key,
		Value:     val,
		Tombstone: tombstone,
		Seq:       seq,
		ExpiresAt: expiresAt,
		Kind:      kind,
		Structure: structure,
		Op:        op,
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
	minLenI, err := checkedChunkLen(minLen, len(payload0)-off, "summary minKey")
	if err != nil {
		return summaryMeta{}, fmt.Errorf("summary minKey out of bounds in %s", path)
	}
	minKey := string(payload0[off : off+minLenI])
	off += minLenI

	maxLen, err := readUvarintAt(payload0, &off)
	if err != nil {
		return summaryMeta{}, err
	}
	maxLenI, err := checkedChunkLen(maxLen, len(payload0)-off, "summary maxKey")
	if err != nil {
		return summaryMeta{}, fmt.Errorf("summary maxKey out of bounds in %s", path)
	}
	maxKey := string(payload0[off : off+maxLenI])
	off += maxLenI

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
		if shared > uint64(len(prevKey)) {
			return nil, fmt.Errorf("summary shared prefix out of range")
		}
		sharedI := int(shared)
		suffixLenI, err := checkedChunkLen(suffixLen, len(payload)-off, "summary suffix")
		if err != nil {
			return nil, fmt.Errorf("summary suffix out of bounds")
		}

		suffix := string(payload[off : off+suffixLenI])
		off += suffixLenI
		key := prevKey[:sharedI] + suffix

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
			if shared > uint64(len(prevKey)) {
				return nil, fmt.Errorf("index shared prefix out of range")
			}
			sharedI := int(shared)
			suffixLenI, err := checkedChunkLen(suffixLen, len(payload)-off, "index suffix")
			if err != nil {
				return nil, fmt.Errorf("index suffix out of bounds")
			}

			suffix := string(payload[off : off+suffixLenI])
			off += suffixLenI
			key := prevKey[:sharedI] + suffix

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

func checkedChunkLen(n uint64, available int, what string) (int, error) {

	if available < 0 {
		return 0, fmt.Errorf("%s out of bounds", what)
	}
	if n > uint64(available) {
		return 0, fmt.Errorf("%s out of bounds", what)
	}
	return int(n), nil
}

func isExpired(rec model.Record, now uint64) bool {

	return rec.ExpiresAt > 0 && rec.ExpiresAt <= now
}

// ---------- LSM helpers ----------

// ListDataFilesInDir returns .data file paths sorted newest-first from the given directory.
func (m *Manager) ListDataFilesInDir(dir string) ([]string, error) {

	pattern := filepath.Join(dir, "sst_*.data")
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
			ts:   parseSSTTimestampFromBase(name),
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

// GetLatestKVRecordFromDir searches for the latest KV record for a key
// across all SSTables in the given directory (newest first).
// It returns the newest KV record even when it is tombstoned/expired.
func (m *Manager) GetLatestKVRecordFromDir(dir string, key string) (model.Record, bool, error) {

	dataFiles, err := m.ListDataFilesInDir(dir)
	if err != nil {
		return model.Record{}, false, err
	}
	if len(dataFiles) == 0 {
		return model.Record{}, false, nil
	}

	for _, dataPath := range dataFiles {
		maybeInFilter, err := m.maybeKeyInFilter(dataPath, key)
		if err != nil {
			return model.Record{}, false, err
		}
		if !maybeInFilter {
			continue
		}

		rec, found, err := m.getLatestKVFromDataFile(dataPath, key)
		if err != nil {
			return model.Record{}, false, err
		}
		if !found {
			continue
		}
		return rec, true, nil
	}

	return model.Record{}, false, nil
}

// GetRecordFromDir searches for a key across all SSTables in the given directory (newest first).
func (m *Manager) GetRecordFromDir(dir string, key string) ([]byte, bool, error) {

	rec, found, err := m.GetLatestKVRecordFromDir(dir, key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	now := uint64(time.Now().Unix())
	if rec.Tombstone || isExpired(rec, now) {
		return nil, false, nil
	}
	return rec.Value, true, nil
}

// GetMergeOperandsFromDir collects merge operands from all SSTables in the given directory.
func (m *Manager) GetMergeOperandsFromDir(dir string, structure model.StructureType, key string) ([]model.Record, error) {

	dataFiles, err := m.ListDataFilesInDir(dir)
	if err != nil {
		return nil, err
	}
	if len(dataFiles) == 0 {
		return nil, nil
	}

	now := uint64(time.Now().Unix())
	var ops []model.Record

	for _, dataPath := range dataFiles {
		// Bloom filter check – skip SSTable if key definitely not present.
		maybeInFilter, err := m.maybeKeyInFilter(dataPath, key)
		if err != nil {
			return nil, err
		}
		if !maybeInFilter {
			continue
		}

		recs, found, err := m.getKeyRecordsFromDataFile(dataPath, key)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}

		for _, rec := range recs {
			if rec.Kind != model.RecordKindMergeOperand {
				continue
			}
			if rec.Structure != structure {
				continue
			}
			if rec.Op != model.MergeOpAdd {
				continue
			}
			if isExpired(rec, now) {
				continue
			}
			ops = append(ops, rec)
		}
	}
	return ops, nil
}

// ReadAllRecordsFromFile reads all records from a .data file in sorted order.
// This is used during compaction to merge SSTable contents.
func (m *Manager) ReadAllRecordsFromFile(dataPath string) ([]model.Record, error) {

	dataHdr, err := m.readFileHeader(dataPath)
	if err != nil {
		return nil, err
	}
	if dataHdr.magic != m.dataMagic {
		return nil, fmt.Errorf("invalid data magic in %s", dataPath)
	}

	blockCount, err := m.countBlocks(dataPath, dataHdr.blockSize)
	if err != nil {
		return nil, err
	}

	prevKey := ""
	var pending []byte
	var records []model.Record

	for blockNo := uint64(0); blockNo < blockCount; blockNo++ {
		payload, err := m.readPayloadBlock(dataPath, dataHdr.blockSize, blockNo)
		if err != nil {
			return nil, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return nil, fmt.Errorf("data header too short in %s", dataPath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return nil, fmt.Errorf("invalid data magic in %s", dataPath)
			}
			off = 8
		}

		for off < len(payload) {
			flags := payload[off]
			fragType := flags & fragTypeMask

			if len(pending) == 0 {
				switch fragType {
				case 0b00:
					rec, consumed, err := decodeDataRecord(payload[off:], prevKey)
					if err != nil {
						return nil, err
					}
					off += consumed
					prevKey = rec.Key
					records = append(records, rec)

				case 0b10:
					pending = append(pending, payload[off:]...)
					off = len(payload)

				case 0b01, 0b11:
					return nil, fmt.Errorf("unexpected continuation fragment in %s block %d", dataPath, blockNo)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return nil, fmt.Errorf("expected continuation fragment in %s block %d", dataPath, blockNo)
			}

			off++
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return nil, fmt.Errorf("fragment chunk out of bounds in %s block %d", dataPath, blockNo)
			}
			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return nil, err
				}
				if consumed != len(pending) {
					return nil, fmt.Errorf("fragment assembly has extra bytes in %s block %d", dataPath, blockNo)
				}
				pending = pending[:0]
				prevKey = rec.Key
				records = append(records, rec)
			}
		}
	}

	if len(pending) != 0 {
		return nil, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return records, nil
}

// GetDirTotalSize returns the total size (in bytes) of all .data files in a directory.
// Uses os.Stat which is metadata-only (no I/O through BlockManager needed for file size).
func (m *Manager) GetDirTotalSize(dir string) (int64, error) {

	pattern := filepath.Join(dir, "sst_*.data")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, p := range matches {
		info, err := os.Stat(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// CollectKVRangeFromDir collects KV records in key range from all SSTables in dir.
// endKey is optional; if endKey == "" range is unbounded on the right side.
// When endExclusive is true, range is [startKey, endKey); otherwise [startKey, endKey].
func (m *Manager) CollectKVRangeFromDir(dir, startKey, endKey string, endExclusive bool) ([]model.Record, error) {

	dataFiles, err := m.ListDataFilesInDir(dir)
	if err != nil {
		return nil, err
	}
	if len(dataFiles) == 0 {
		return nil, nil
	}

	out := make([]model.Record, 0)
	for _, dataPath := range dataFiles {
		blockSize, startDataBlock, endDataBlock, ok, err := m.locateDataRangeForScan(dataPath, startKey, endKey, endExclusive)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		recs, err := m.scanDataRangeForKV(dataPath, blockSize, startDataBlock, endDataBlock, startKey, endKey, endExclusive, "")
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// CollectKVPrefixFromDir collects KV records whose key starts with prefix from all SSTables in dir.
func (m *Manager) CollectKVPrefixFromDir(dir, prefix string) ([]model.Record, error) {

	upper, hasUpper := prefixUpperBound(prefix)

	dataFiles, err := m.ListDataFilesInDir(dir)
	if err != nil {
		return nil, err
	}
	if len(dataFiles) == 0 {
		return nil, nil
	}

	out := make([]model.Record, 0)
	for _, dataPath := range dataFiles {
		endKey := ""
		if hasUpper {
			endKey = upper
		}

		blockSize, startDataBlock, endDataBlock, ok, err := m.locateDataRangeForScan(dataPath, prefix, endKey, true)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		recs, err := m.scanDataRangeForKV(dataPath, blockSize, startDataBlock, endDataBlock, prefix, endKey, true, prefix)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

func (m *Manager) locateDataRangeForScan(dataPath, startKey, endKey string, endExclusive bool) (blockSize int, startDataBlock, endDataBlock uint64, ok bool, err error) {

	basePath := strings.TrimSuffix(dataPath, ".data")
	indexPath := basePath + ".index"
	summaryPath := basePath + ".summary"

	dataHdr, err := m.readFileHeader(dataPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if dataHdr.magic != m.dataMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid data magic in %s", dataPath)
	}

	indexHdr, err := m.readFileHeader(indexPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if indexHdr.magic != m.indexMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid index magic in %s", indexPath)
	}

	summaryHdr, err := m.readFileHeader(summaryPath)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if summaryHdr.magic != m.summMagic {
		return 0, 0, 0, false, fmt.Errorf("invalid summary magic in %s", summaryPath)
	}

	summ, err := m.readSummaryMeta(summaryPath, summaryHdr.blockSize, startKey)
	if err != nil {
		return 0, 0, 0, false, err
	}

	if !rangeOverlapsTable(startKey, endKey, endExclusive, summ.minKey, summ.maxKey) {
		return 0, 0, 0, false, nil
	}

	startIndexBlock := uint64(0)
	if startKey != "" {
		for _, se := range summ.entries {
			if se.key < startKey {
				startIndexBlock = se.indexBlockNo
				continue
			}
			break
		}
	}

	if startKey == "" {
		startDataBlock = 0
	} else {
		indexEntries, err := m.readIndexEntriesFromBlock(indexPath, indexHdr.blockSize, startIndexBlock, startKey)
		if err != nil {
			return 0, 0, 0, false, err
		}
		var (
			hasPrev   bool
			predBlock uint64
		)
		for _, ie := range indexEntries {
			if ie.key < startKey {
				hasPrev = true
				predBlock = ie.dataBlockNo
				continue
			}
			break
		}
		if hasPrev {
			startDataBlock = predBlock
		} else {
			startDataBlock = 0
		}
	}

	endDataBlock, err = m.countBlocks(dataPath, dataHdr.blockSize)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if startDataBlock >= endDataBlock {
		return 0, 0, 0, false, nil
	}

	return dataHdr.blockSize, startDataBlock, endDataBlock, true, nil
}

func (m *Manager) scanDataRangeForKV(dataPath string, blockSize int, startBlock, endBlock uint64, startKey, endKey string, endExclusive bool, prefix string) ([]model.Record, error) {

	prevKey := ""
	var pending []byte
	out := make([]model.Record, 0)
	reachedPrefixWindow := prefix == ""

	for blockNo := startBlock; blockNo < endBlock; blockNo++ {
		payload, err := m.readPayloadBlock(dataPath, blockSize, blockNo)
		if err != nil {
			return nil, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return nil, fmt.Errorf("data header too short in %s", dataPath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return nil, fmt.Errorf("invalid data magic in %s", dataPath)
			}
			off = 8
		}

		for off < len(payload) {
			flags := payload[off]
			fragType := flags & fragTypeMask

			if len(pending) == 0 {
				switch fragType {
				case 0b00:
					rec, consumed, err := decodeDataRecord(payload[off:], prevKey)
					if err != nil {
						return nil, err
					}
					off += consumed
					prevKey = rec.Key

					if startKey != "" && rec.Key < startKey {
						continue
					}

					if endKey != "" {
						if endExclusive {
							if rec.Key >= endKey {
								return out, nil
							}
						} else {
							if rec.Key > endKey {
								return out, nil
							}
						}
					}

					if prefix != "" {
						if rec.Key < prefix {
							continue
						}
						if strings.HasPrefix(rec.Key, prefix) {
							reachedPrefixWindow = true
						} else if reachedPrefixWindow {
							return out, nil
						} else {
							continue
						}
					}

					if rec.Kind != model.RecordKindKV {
						continue
					}
					out = append(out, rec)

				case 0b10:
					pending = append(pending, payload[off:]...)
					off = len(payload)

				case 0b01, 0b11:
					return nil, fmt.Errorf("unexpected continuation fragment in %s block %d", dataPath, blockNo)

				default:
					return nil, fmt.Errorf("unknown fragment flag %d in %s block %d", fragType, dataPath, blockNo)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return nil, fmt.Errorf("expected continuation fragment in %s block %d", dataPath, blockNo)
			}

			off++
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return nil, fmt.Errorf("fragment chunk out of bounds in %s block %d", dataPath, blockNo)
			}
			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return nil, err
				}
				if consumed != len(pending) {
					return nil, fmt.Errorf("fragment assembly has extra bytes in %s block %d", dataPath, blockNo)
				}
				pending = pending[:0]
				prevKey = rec.Key

				if startKey != "" && rec.Key < startKey {
					continue
				}

				if endKey != "" {
					if endExclusive {
						if rec.Key >= endKey {
							return out, nil
						}
					} else {
						if rec.Key > endKey {
							return out, nil
						}
					}
				}

				if prefix != "" {
					if rec.Key < prefix {
						continue
					}
					if strings.HasPrefix(rec.Key, prefix) {
						reachedPrefixWindow = true
					} else if reachedPrefixWindow {
						return out, nil
					} else {
						continue
					}
				}

				if rec.Kind != model.RecordKindKV {
					continue
				}
				out = append(out, rec)
			}
		}
	}

	if len(pending) != 0 {
		return nil, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return out, nil
}

func rangeOverlapsTable(startKey, endKey string, endExclusive bool, tableMin, tableMax string) bool {

	if tableMax != "" && startKey != "" && startKey > tableMax {
		return false
	}
	if endKey != "" && tableMin != "" {
		if endExclusive {
			if endKey <= tableMin {
				return false
			}
		} else {
			if endKey < tableMin {
				return false
			}
		}
	}
	return true
}

func prefixUpperBound(prefix string) (string, bool) {

	if prefix == "" {
		return "", false
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			out := make([]byte, i+1)
			copy(out, b[:i+1])
			out[i]++
			return string(out), true
		}
	}
	return "", false
}
