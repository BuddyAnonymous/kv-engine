package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"

	"kv-engine/internal/model"
)

const probMetaBlockSize = 4 * 1024

func (m *Manager) AppendProbMeta(rec model.ProbMetaRecord) error {
	if rec.Key == "" {
		return errors.New("probmeta key is empty")
	}
	if rec.Structure == model.StructureTypeNone {
		return errors.New("invalid probmeta structure type")
	}
	if rec.Action != model.ProbMetaActionCreate && rec.Action != model.ProbMetaActionDelete {
		return errors.New("invalid probmeta action")
	}

	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}

	enc, err := encodeProbMetaRecord(rec)
	if err != nil {
		return err
	}
	item := append(uvarintBytes(uint64(len(enc))), enc...)
	maxPayload := probMetaBlockSize - crcBytes - payloadLenBytes
	if len(item) > maxPayload {
		return fmt.Errorf("probmeta record too large: len=%d maxPayload=%d", len(item), maxPayload)
	}
	path := m.probMetaPath()

	blockCount, err := m.countBlocks(path, probMetaBlockSize)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && blockCount > 0 {
		lastBlockNo := blockCount - 1
		blockData, err := m.bm.ReadBlock(path, lastBlockNo, probMetaBlockSize)
		if err != nil {
			return err
		}
		payloadLen := int(binary.LittleEndian.Uint32(blockData[:payloadLenBytes]))
		if payloadLen < 0 || payloadLen > maxPayload {
			return fmt.Errorf("invalid probmeta payload length at %s block %d: %d", path, lastBlockNo, payloadLen)
		}
		crcWant := binary.LittleEndian.Uint32(blockData[probMetaBlockSize-crcBytes:])
		crcGot := crc32.ChecksumIEEE(blockData[:probMetaBlockSize-crcBytes])
		if crcGot != crcWant {
			return fmt.Errorf("crc mismatch at %s block %d", path, lastBlockNo)
		}

		payload := blockData[payloadLenBytes : payloadLenBytes+payloadLen]
		isPacked := payloadLen == 0 || decodeProbMetaPayload(payload, func(model.ProbMetaRecord) error { return nil }) == nil
		if isPacked && payloadLen+len(item) <= maxPayload {
			copy(blockData[payloadLenBytes+payloadLen:], item)
			payloadLen += len(item)
			binary.LittleEndian.PutUint32(blockData[:payloadLenBytes], uint32(payloadLen))
			crc := crc32.ChecksumIEEE(blockData[:probMetaBlockSize-crcBytes])
			binary.LittleEndian.PutUint32(blockData[probMetaBlockSize-crcBytes:], crc)
			return m.bm.WriteBlock(path, lastBlockNo, blockData, probMetaBlockSize)
		}
	}

	blockData := make([]byte, probMetaBlockSize)
	binary.LittleEndian.PutUint32(blockData[:payloadLenBytes], uint32(len(item)))
	copy(blockData[payloadLenBytes:payloadLenBytes+len(item)], item)

	crc := crc32.ChecksumIEEE(blockData[:probMetaBlockSize-crcBytes])
	binary.LittleEndian.PutUint32(blockData[probMetaBlockSize-crcBytes:], crc)

	_, err = m.bm.AppendBlock(path, blockData, probMetaBlockSize)
	return err
}

func (m *Manager) GetLatestProbMeta(structure model.StructureType, key string) (model.ProbMetaRecord, bool, error) {
	p := m.probMetaPath()
	blockCount, err := m.countBlocks(p, probMetaBlockSize)
	if err != nil {
		if os.IsNotExist(err) {
			return model.ProbMetaRecord{}, false, nil
		}
		return model.ProbMetaRecord{}, false, err
	}
	var (
		best  model.ProbMetaRecord
		found bool
	)
	maxPayload := probMetaBlockSize - crcBytes - payloadLenBytes
	for blockNo := uint64(0); blockNo < blockCount; blockNo++ {
		blockData, err := m.bm.ReadBlock(p, blockNo, probMetaBlockSize)
		if err != nil {
			return model.ProbMetaRecord{}, false, err
		}
		payloadLen := int(binary.LittleEndian.Uint32(blockData[:payloadLenBytes]))
		if payloadLen < 0 || payloadLen > maxPayload {
			return model.ProbMetaRecord{}, false, fmt.Errorf("invalid probmeta payload length at %s block %d: %d", p, blockNo, payloadLen)
		}

		crcWant := binary.LittleEndian.Uint32(blockData[probMetaBlockSize-crcBytes:])
		crcGot := crc32.ChecksumIEEE(blockData[:probMetaBlockSize-crcBytes])
		if crcGot != crcWant {
			return model.ProbMetaRecord{}, false, fmt.Errorf("crc mismatch at %s block %d", p, blockNo)
		}
		if payloadLen == 0 {
			continue
		}
		payload := make([]byte, payloadLen)
		copy(payload, blockData[payloadLenBytes:payloadLenBytes+payloadLen])

		err = decodeProbMetaPayload(payload, func(rec model.ProbMetaRecord) error {
			if rec.Structure != structure || rec.Key != key {
				return nil
			}
			if !found || rec.Seq > best.Seq {
				best = rec
				found = true
			}
			return nil
		})
		if err != nil {
			return model.ProbMetaRecord{}, false, err
		}
	}
	return best, found, nil
}

func (m *Manager) MaxProbMetaSeq() (uint64, error) {
	p := m.probMetaPath()
	blockCount, err := m.countBlocks(p, probMetaBlockSize)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var maxSeq uint64
	maxPayload := probMetaBlockSize - crcBytes - payloadLenBytes
	for blockNo := uint64(0); blockNo < blockCount; blockNo++ {
		blockData, err := m.bm.ReadBlock(p, blockNo, probMetaBlockSize)
		if err != nil {
			return 0, err
		}

		payloadLen := int(binary.LittleEndian.Uint32(blockData[:payloadLenBytes]))
		if payloadLen < 0 || payloadLen > maxPayload {
			return 0, fmt.Errorf("invalid probmeta payload length at %s block %d: %d", p, blockNo, payloadLen)
		}
		crcWant := binary.LittleEndian.Uint32(blockData[probMetaBlockSize-crcBytes:])
		crcGot := crc32.ChecksumIEEE(blockData[:probMetaBlockSize-crcBytes])
		if crcGot != crcWant {
			return 0, fmt.Errorf("crc mismatch at %s block %d", p, blockNo)
		}
		if payloadLen == 0 {
			continue
		}

		payload := make([]byte, payloadLen)
		copy(payload, blockData[payloadLenBytes:payloadLenBytes+payloadLen])
		if err := decodeProbMetaPayload(payload, func(rec model.ProbMetaRecord) error {
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return maxSeq, nil
}

func (m *Manager) probMetaPath() string {
	return filepath.Join(m.dir, "sst.probmeta")
}

func encodeProbMetaRecord(rec model.ProbMetaRecord) ([]byte, error) {
	action, err := encodeProbMetaAction(rec.Action)
	if err != nil {
		return nil, err
	}
	if rec.Structure > 0b11 {
		return nil, errors.New("invalid probmeta structure type")
	}
	flags := byte(rec.Structure&0b11) | ((action & 0b11) << 2)

	var b bytes.Buffer
	b.WriteByte(flags)
	b.Write(uvarintBytes(uint64(len(rec.Key))))
	b.WriteString(rec.Key)
	b.Write(uvarintBytes(rec.Seq))

	b.Write(uvarintBytes(rec.BFExpectedElements))
	b.Write(uvarintBytes(math.Float64bits(rec.BFFalsePositiveRate)))
	b.Write(uvarintBytes(uint64(rec.BFSeed)))

	b.Write(uvarintBytes(math.Float64bits(rec.CMSEpsilon)))
	b.Write(uvarintBytes(math.Float64bits(rec.CMSDelta)))
	b.Write(uvarintBytes(uint64(rec.CMSSeed)))

	b.WriteByte(rec.HLLPrecision)
	b.Write(uvarintBytes(uint64(rec.HLLSeed)))
	return b.Bytes(), nil
}

func decodeProbMetaRecord(payload []byte) (model.ProbMetaRecord, error) {
	off := 0
	if len(payload) < 1 {
		return model.ProbMetaRecord{}, errors.New("invalid probmeta payload")
	}

	flags := payload[off]
	structure := model.StructureType(flags & 0b11)
	actionCode := (flags >> 2) & 0b11
	off++

	keyLen, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	if keyLen > uint64(len(payload)-off) {
		return model.ProbMetaRecord{}, errors.New("invalid probmeta key length")
	}
	key := string(payload[off : off+int(keyLen)])
	off += int(keyLen)
	action, err := decodeProbMetaAction(actionCode)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}

	seq, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	bfExpected, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	bfFprBits, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	bfSeed, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	cmsEpsBits, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	cmsDeltaBits, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	cmsSeed, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	if off >= len(payload) {
		return model.ProbMetaRecord{}, errors.New("invalid probmeta payload")
	}
	hllPrecision := payload[off]
	off++
	hllSeed, err := readUvarintFromBytes(payload, &off)
	if err != nil {
		return model.ProbMetaRecord{}, err
	}
	if off != len(payload) {
		return model.ProbMetaRecord{}, errors.New("invalid probmeta trailing bytes")
	}

	return model.ProbMetaRecord{
		Structure:           structure,
		Key:                 key,
		Action:              action,
		Seq:                 seq,
		BFExpectedElements:  bfExpected,
		BFFalsePositiveRate: math.Float64frombits(bfFprBits),
		BFSeed:              uint32(bfSeed),
		CMSEpsilon:          math.Float64frombits(cmsEpsBits),
		CMSDelta:            math.Float64frombits(cmsDeltaBits),
		CMSSeed:             uint32(cmsSeed),
		HLLPrecision:        hllPrecision,
		HLLSeed:             uint32(hllSeed),
	}, nil
}

func encodeProbMetaAction(a model.ProbMetaAction) (byte, error) {
	switch a {
	case model.ProbMetaActionCreate:
		return 1, nil
	case model.ProbMetaActionDelete:
		return 2, nil
	default:
		return 0, errors.New("invalid probmeta action")
	}
}

func decodeProbMetaAction(b byte) (model.ProbMetaAction, error) {
	switch b {
	case 1:
		return model.ProbMetaActionCreate, nil
	case 2:
		return model.ProbMetaActionDelete, nil
	default:
		return "", errors.New("invalid probmeta action")
	}
}

func readUvarintFromBytes(b []byte, off *int) (uint64, error) {
	if *off >= len(b) {
		return 0, errors.New("uvarint offset out of range")
	}
	v, n := binary.Uvarint(b[*off:])
	if n <= 0 {
		return 0, errors.New("invalid uvarint encoding")
	}
	*off += n
	return v, nil
}

func decodeProbMetaPayload(payload []byte, onRecord func(model.ProbMetaRecord) error) error {
	off := 0
	for off < len(payload) {
		recLen, err := readUvarintFromBytes(payload, &off)
		if err != nil {
			return err
		}
		if recLen == 0 {
			continue
		}
		if recLen > uint64(len(payload)-off) {
			return errors.New("probmeta record length out of bounds")
		}
		recBytes := payload[off : off+int(recLen)]
		off += int(recLen)

		rec, err := decodeProbMetaRecord(recBytes)
		if err != nil {
			return err
		}
		if err := onRecord(rec); err != nil {
			return err
		}
	}
	return nil
}
