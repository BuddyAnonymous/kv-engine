package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"time"

	"kv-engine/internal/model"
	"kv-engine/internal/probabilistic/bloom"
)

type singleFileFooter struct {
	DataOffset    uint64
	DataLen       uint64
	IndexOffset   uint64
	IndexLen      uint64
	SummaryOffset uint64
	SummaryLen    uint64
	FilterOffset  uint64
	FilterLen     uint64
	MerkleOffset  uint64
	MerkleLen     uint64
}

func (m *Manager) writeSingleFile(path string, records []model.Record) error {
	if m.bm == nil {
		return fmt.Errorf("block manager is nil")
	}
	if m.blockSize <= 0 {
		return fmt.Errorf("invalid blockSize=%d", m.blockSize)
	}
	if len(records) == 0 {
		return nil
	}

	basePath := path
	basePath = strings.TrimSuffix(basePath, ".sst")
	singlePath := basePath + ".sst"
	tmpPrefix := fmt.Sprintf("%s.single_tmp_%d", basePath, time.Now().UnixNano())

	_ = os.Remove(singlePath)

	dataTmp := tmpPrefix + ".data"
	indexTmp := tmpPrefix + ".index"
	summTmp := tmpPrefix + ".summary"
	filterTmp := tmpPrefix + ".filter"
	merkleTmp := tmpPrefix + ".merkle"

	cleanupPaths := []string{dataTmp, indexTmp, summTmp, filterTmp, merkleTmp}
	defer func() {
		for _, p := range cleanupPaths {
			_ = os.Remove(p)
		}
	}()

	dataFirstKeys, err := m.writeDataFile(dataTmp, records)
	if err != nil {
		return err
	}
	indexEntryLoc, err := m.writeIndexFile(indexTmp, dataFirstKeys)
	if err != nil {
		return err
	}
	if err := m.writeSummaryFile(summTmp, indexEntryLoc, dataFirstKeys, records[len(records)-1].Key); err != nil {
		return err
	}
	if err := m.writeFilterFile(filterTmp, records); err != nil {
		return err
	}
	if err := m.writeMerkleFile(merkleTmp, records); err != nil {
		return err
	}

	footer := singleFileFooter{}
	var nextBlock uint64

	footer.DataOffset = nextBlock
	footer.DataLen, err = m.copyBlocks(dataTmp, singlePath, nextBlock)
	if err != nil {
		return err
	}
	nextBlock += footer.DataLen

	footer.IndexOffset = nextBlock
	footer.IndexLen, err = m.copyBlocks(indexTmp, singlePath, nextBlock)
	if err != nil {
		return err
	}
	nextBlock += footer.IndexLen

	footer.SummaryOffset = nextBlock
	footer.SummaryLen, err = m.copyBlocks(summTmp, singlePath, nextBlock)
	if err != nil {
		return err
	}
	nextBlock += footer.SummaryLen

	footer.FilterOffset = nextBlock
	footer.FilterLen, err = m.copyBlocks(filterTmp, singlePath, nextBlock)
	if err != nil {
		return err
	}
	nextBlock += footer.FilterLen

	footer.MerkleOffset = nextBlock
	footer.MerkleLen, err = m.copyBlocks(merkleTmp, singlePath, nextBlock)
	if err != nil {
		return err
	}
	nextBlock += footer.MerkleLen

	if err := m.writeSingleFooter(singlePath, nextBlock, footer); err != nil {
		return err
	}
	return m.writeTOC(basePath, tocModeSingle)
}

func (m *Manager) copyBlocks(srcPath, dstPath string, dstStartBlock uint64) (uint64, error) {
	blockCount, err := m.countBlocks(srcPath, m.blockSize)
	if err != nil {
		return 0, err
	}
	for i := uint64(0); i < blockCount; i++ {
		b, err := m.bm.ReadBlock(srcPath, i, m.blockSize)
		if err != nil {
			return 0, err
		}
		if err := m.bm.WriteBlock(dstPath, dstStartBlock+i, b, m.blockSize); err != nil {
			return 0, err
		}
	}
	return blockCount, nil
}

func (m *Manager) encodeSingleFooterPayload(f singleFileFooter) []byte {
	payload := make([]byte, 0, 8+10*10)
	payload = append(payload, m.encodeHeader(m.singleMagic)...)
	payload = append(payload, uvarintBytes(f.DataOffset)...)
	payload = append(payload, uvarintBytes(f.DataLen)...)
	payload = append(payload, uvarintBytes(f.IndexOffset)...)
	payload = append(payload, uvarintBytes(f.IndexLen)...)
	payload = append(payload, uvarintBytes(f.SummaryOffset)...)
	payload = append(payload, uvarintBytes(f.SummaryLen)...)
	payload = append(payload, uvarintBytes(f.FilterOffset)...)
	payload = append(payload, uvarintBytes(f.FilterLen)...)
	payload = append(payload, uvarintBytes(f.MerkleOffset)...)
	payload = append(payload, uvarintBytes(f.MerkleLen)...)
	return payload
}

func (m *Manager) writeSingleFooter(path string, footerBlock uint64, f singleFileFooter) error {
	payload := m.encodeSingleFooterPayload(f)
	maxPayload := m.blockSize - crcBytes - payloadLenBytes
	if len(payload) > maxPayload {
		return fmt.Errorf("single footer too large for block payload: need=%d cap=%d", len(payload), maxPayload)
	}

	blockData := make([]byte, m.blockSize)
	copy(blockData[payloadLenBytes:], payload)
	binary.LittleEndian.PutUint32(blockData[0:payloadLenBytes], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(blockData[:m.blockSize-crcBytes])
	binary.LittleEndian.PutUint32(blockData[m.blockSize-crcBytes:], crc)
	return m.bm.WriteBlock(path, footerBlock, blockData, m.blockSize)
}

func (m *Manager) readSingleFooter(path string) (singleFileFooter, int, error) {
	hdr, err := m.readFileHeader(path)
	if err != nil {
		return singleFileFooter{}, 0, err
	}
	blockCount, err := m.countBlocks(path, hdr.blockSize)
	if err != nil {
		return singleFileFooter{}, 0, err
	}
	if blockCount == 0 {
		return singleFileFooter{}, 0, fmt.Errorf("single sstable is empty: %s", path)
	}
	payload, err := m.readPayloadBlock(path, hdr.blockSize, blockCount-1)
	if err != nil {
		return singleFileFooter{}, 0, err
	}
	if len(payload) < 8 {
		return singleFileFooter{}, 0, fmt.Errorf("single footer too short in %s", path)
	}
	if string(payload[:4]) != string(m.singleMagic[:]) {
		return singleFileFooter{}, 0, fmt.Errorf("invalid single footer magic in %s", path)
	}
	off := 8
	readField := func() (uint64, error) {
		return readUvarintAt(payload, &off)
	}
	var f singleFileFooter
	if f.DataOffset, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.DataLen, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.IndexOffset, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.IndexLen, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.SummaryOffset, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.SummaryLen, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.FilterOffset, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.FilterLen, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.MerkleOffset, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if f.MerkleLen, err = readField(); err != nil {
		return singleFileFooter{}, 0, err
	}
	if off != len(payload) {
		return singleFileFooter{}, 0, fmt.Errorf("single footer trailing bytes in %s", path)
	}

	if f.DataLen == 0 {
		return singleFileFooter{}, 0, fmt.Errorf("single footer has empty data section in %s", path)
	}
	return f, hdr.blockSize, nil
}

func (m *Manager) maybeKeyInSingleFilter(singlePath string, footer singleFileFooter, blockSize int, key string) (bool, error) {
	if footer.FilterLen == 0 {
		return true, nil
	}
	var payload []byte
	for i := uint64(0); i < footer.FilterLen; i++ {
		chunk, err := m.readPayloadBlock(singlePath, blockSize, footer.FilterOffset+i)
		if err != nil {
			return false, err
		}
		payload = append(payload, chunk...)
	}
	if len(payload) < 8 {
		return false, fmt.Errorf("single filter payload too short in %s", singlePath)
	}
	if string(payload[:4]) != string(m.filterMagic[:]) {
		return false, fmt.Errorf("invalid filter magic in single file %s", singlePath)
	}
	bf, err := bloom.Deserialize(payload[8:])
	if err != nil {
		return false, err
	}
	return bf.MightContain([]byte(key)), nil
}

func (m *Manager) getLatestKVFromSingleFile(singlePath string, footer singleFileFooter, blockSize int, key string) (model.Record, bool, error) {
	startDataBlock, endDataBlock, ok, err := m.locateSingleDataRangeForAllKeyRecords(singlePath, footer, blockSize, key)
	if err != nil {
		return model.Record{}, false, err
	}
	if !ok {
		return model.Record{}, false, nil
	}
	return m.searchSingleDataRangeForLatestKV(singlePath, footer, blockSize, key, startDataBlock, endDataBlock)
}

func (m *Manager) locateSingleDataRangeForAllKeyRecords(singlePath string, footer singleFileFooter, blockSize int, key string) (startDataBlock, endDataBlock uint64, ok bool, err error) {
	summ, err := m.readSingleSummaryMeta(singlePath, footer, blockSize, key)
	if err != nil {
		return 0, 0, false, err
	}
	if summ.minKey != "" && key < summ.minKey {
		return 0, 0, false, nil
	}
	if summ.maxKey != "" && key > summ.maxKey {
		return 0, 0, false, nil
	}

	startIndexBlock := uint64(0)
	for _, se := range summ.entries {
		if se.key < key {
			startIndexBlock = se.indexBlockNo
			continue
		}
		break
	}

	indexEntries, err := m.readSingleIndexEntriesFromBlock(singlePath, footer, blockSize, startIndexBlock, key)
	if err != nil {
		return 0, 0, false, err
	}
	if len(indexEntries) == 0 {
		return 0, 0, false, nil
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

	endDataBlock = footer.DataLen
	if startDataBlock >= endDataBlock {
		return 0, 0, false, fmt.Errorf("invalid single data block range [%d,%d) for %s", startDataBlock, endDataBlock, singlePath)
	}
	return startDataBlock, endDataBlock, true, nil
}

func (m *Manager) readSingleSummaryMeta(singlePath string, footer singleFileFooter, blockSize int, key string) (summaryMeta, error) {
	if footer.SummaryLen == 0 {
		return summaryMeta{}, fmt.Errorf("single file has empty summary section: %s", singlePath)
	}

	payload0, err := m.readPayloadBlock(singlePath, blockSize, footer.SummaryOffset)
	if err != nil {
		return summaryMeta{}, err
	}
	if len(payload0) < 8 {
		return summaryMeta{}, fmt.Errorf("single summary header too short in %s", singlePath)
	}
	if string(payload0[:4]) != string(m.summMagic[:]) {
		return summaryMeta{}, fmt.Errorf("invalid single summary magic in %s", singlePath)
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
	minLenI, err := checkedChunkLen(minLen, len(payload0)-off, "single summary minKey")
	if err != nil {
		return summaryMeta{}, fmt.Errorf("single summary minKey out of bounds in %s", singlePath)
	}
	minKey := string(payload0[off : off+minLenI])
	off += minLenI

	maxLen, err := readUvarintAt(payload0, &off)
	if err != nil {
		return summaryMeta{}, err
	}
	maxLenI, err := checkedChunkLen(maxLen, len(payload0)-off, "single summary maxKey")
	if err != nil {
		return summaryMeta{}, fmt.Errorf("single summary maxKey out of bounds in %s", singlePath)
	}
	maxKey := string(payload0[off : off+maxLenI])
	off += maxLenI

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

	for i := uint64(1); i < footer.SummaryLen; i++ {
		payload, err := m.readPayloadBlock(singlePath, blockSize, footer.SummaryOffset+i)
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

func (m *Manager) readSingleIndexEntriesFromBlock(singlePath string, footer singleFileFooter, blockSize int, startBlock uint64, targetKey string) ([]indexEntry, error) {
	if footer.IndexLen == 0 || startBlock >= footer.IndexLen {
		return nil, nil
	}

	out := make([]indexEntry, 0)
	for relBlock := startBlock; relBlock < footer.IndexLen; relBlock++ {
		payload, err := m.readPayloadBlock(singlePath, blockSize, footer.IndexOffset+relBlock)
		if err != nil {
			return nil, err
		}

		off := 0
		if relBlock == 0 {
			if len(payload) < 8 {
				return nil, fmt.Errorf("single index header too short in %s", singlePath)
			}
			if string(payload[:4]) != string(m.indexMagic[:]) {
				return nil, fmt.Errorf("invalid single index magic in %s", singlePath)
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
				return nil, fmt.Errorf("single index shared prefix out of range")
			}
			sharedI := int(shared)
			suffixLenI, err := checkedChunkLen(suffixLen, len(payload)-off, "single index suffix")
			if err != nil {
				return nil, fmt.Errorf("single index suffix out of bounds")
			}

			suffix := string(payload[off : off+suffixLenI])
			off += suffixLenI
			key := prevKey[:sharedI] + suffix

			dataBlockNo, err := readUvarintAt(payload, &off)
			if err != nil {
				return nil, err
			}

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

func (m *Manager) searchSingleDataRangeForLatestKV(singlePath string, footer singleFileFooter, blockSize int, key string, startBlock, endBlock uint64) (model.Record, bool, error) {
	prevKey := ""
	var pending []byte
	var (
		best  model.Record
		found bool
	)

	for relBlock := startBlock; relBlock < endBlock; relBlock++ {
		payload, err := m.readPayloadBlock(singlePath, blockSize, footer.DataOffset+relBlock)
		if err != nil {
			return model.Record{}, false, err
		}

		off := 0
		if relBlock == 0 {
			if len(payload) < 8 {
				return model.Record{}, false, fmt.Errorf("single data header too short in %s", singlePath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return model.Record{}, false, fmt.Errorf("invalid single data magic in %s", singlePath)
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
					return model.Record{}, false, fmt.Errorf("unexpected continuation fragment in %s block %d", singlePath, footer.DataOffset+relBlock)

				default:
					return model.Record{}, false, fmt.Errorf("unknown fragment flag %d in %s block %d", fragType, singlePath, footer.DataOffset+relBlock)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return model.Record{}, false, fmt.Errorf("expected continuation fragment in %s block %d", singlePath, footer.DataOffset+relBlock)
			}

			off++
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return model.Record{}, false, err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return model.Record{}, false, fmt.Errorf("fragment chunk out of bounds in %s block %d", singlePath, footer.DataOffset+relBlock)
			}
			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return model.Record{}, false, err
				}
				if consumed != len(pending) {
					return model.Record{}, false, fmt.Errorf("fragment assembly has extra bytes in %s block %d", singlePath, footer.DataOffset+relBlock)
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
		return model.Record{}, false, fmt.Errorf("unterminated fragmented record in %s", singlePath)
	}
	return best, found, nil
}

func (m *Manager) scanSingleDataSection(singlePath string, blockSize int, footer singleFileFooter, onRec func(model.Record) (bool, error)) error {
	prevKey := ""
	var pending []byte
	start := footer.DataOffset
	end := footer.DataOffset + footer.DataLen

	for absBlock := start; absBlock < end; absBlock++ {
		payload, err := m.readPayloadBlock(singlePath, blockSize, absBlock)
		if err != nil {
			return err
		}
		off := 0
		if absBlock == start {
			if len(payload) < 8 {
				return fmt.Errorf("single data header too short in %s", singlePath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return fmt.Errorf("invalid single data magic in %s", singlePath)
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
						return err
					}
					off += consumed
					prevKey = rec.Key
					stop, err := onRec(rec)
					if err != nil {
						return err
					}
					if stop {
						return nil
					}
				case 0b10:
					pending = append(pending, payload[off:]...)
					off = len(payload)
				case 0b01, 0b11:
					return fmt.Errorf("unexpected continuation fragment in single file %s block %d", singlePath, absBlock)
				default:
					return fmt.Errorf("unknown fragment flag %d in single file %s block %d", fragType, singlePath, absBlock)
				}
				continue
			}

			if fragType != 0b01 && fragType != 0b11 {
				return fmt.Errorf("expected continuation fragment in single file %s block %d", singlePath, absBlock)
			}
			off++
			chunkLen, err := readUvarintAt(payload, &off)
			if err != nil {
				return err
			}
			chunkLenI, err := checkedChunkLen(chunkLen, len(payload)-off, "fragment chunk")
			if err != nil {
				return fmt.Errorf("fragment chunk out of bounds in single file %s block %d", singlePath, absBlock)
			}
			pending = append(pending, payload[off:off+chunkLenI]...)
			off += chunkLenI

			if fragType == 0b01 {
				rec, consumed, err := decodeDataRecord(pending, prevKey)
				if err != nil {
					return err
				}
				if consumed != len(pending) {
					return fmt.Errorf("fragment assembly has extra bytes in single file %s block %d", singlePath, absBlock)
				}
				pending = pending[:0]
				prevKey = rec.Key
				stop, err := onRec(rec)
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
			}
		}
	}
	if len(pending) != 0 {
		return fmt.Errorf("unterminated fragmented record in single file %s", singlePath)
	}
	return nil
}
