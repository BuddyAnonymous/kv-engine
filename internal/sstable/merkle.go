package sstable

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"kv-engine/internal/model"
)

const merkleFormatVersion = 1

func (m *Manager) writeMerkleFile(merklePath string, records []model.Record) error {
	leafHashes := make([][32]byte, 0, len(records))
	for _, rec := range records {
		leafHashes = append(leafHashes, sha256.Sum256(rec.Value))
	}
	root := buildMerkleRoot(leafHashes)

	payload := make([]byte, 0, 8+10+10+32+len(leafHashes)*32)
	payload = append(payload, m.encodeHeader(m.merkleMagic)...)
	payload = append(payload, uvarintBytes(merkleFormatVersion)...)
	payload = append(payload, uvarintBytes(uint64(len(leafHashes)))...)
	payload = append(payload, root[:]...)
	for _, h := range leafHashes {
		payload = append(payload, h[:]...)
	}

	bw := newBlockWriter(m.bm, merklePath, m.blockSize, nil)
	chunkSize := bw.payloadCap()
	for off := 0; off < len(payload); {
		end := off + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		if err := bw.writeBytes(payload[off:end]); err != nil {
			return err
		}
		off = end
	}
	return bw.close()
}

func (m *Manager) ValidateMerkle(table string) (model.MerkleValidationResult, error) {
	basePath, err := m.resolveSSTBasePath(table)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	expRoot, expLeaves, err := m.readMerkleFile(basePath + ".merkle")
	if err != nil {
		return model.MerkleValidationResult{}, err
	}
	values, err := m.readAllDataValues(basePath + ".data")
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	actLeaves := make([][32]byte, 0, len(values))
	for _, v := range values {
		actLeaves = append(actLeaves, sha256.Sum256(v))
	}
	actRoot := buildMerkleRoot(actLeaves)

	changed := diffLeafHashes(expLeaves, actLeaves)
	valid := len(changed) == 0 && expRoot == actRoot

	return model.MerkleValidationResult{
		Valid:              valid,
		ChangedLeafIndices: changed,
		ExpectedRootHex:    hex.EncodeToString(expRoot[:]),
		ActualRootHex:      hex.EncodeToString(actRoot[:]),
		ExpectedLeafCount:  len(expLeaves),
		ActualLeafCount:    len(actLeaves),
	}, nil
}

func (m *Manager) readMerkleFile(path string) ([32]byte, [][32]byte, error) {
	hdr, err := m.readFileHeader(path)
	if err != nil {
		return [32]byte{}, nil, err
	}
	if hdr.magic != m.merkleMagic {
		return [32]byte{}, nil, fmt.Errorf("invalid merkle magic in %s", path)
	}

	payload, err := m.readAllPayload(path, hdr.blockSize)
	if err != nil {
		return [32]byte{}, nil, err
	}
	if len(payload) < 8 {
		return [32]byte{}, nil, fmt.Errorf("merkle header too short in %s", path)
	}
	if string(payload[:4]) != string(m.merkleMagic[:]) {
		return [32]byte{}, nil, fmt.Errorf("invalid merkle payload magic in %s", path)
	}

	off := 8
	version, err := readUvarintAt(payload, &off)
	if err != nil {
		return [32]byte{}, nil, err
	}
	if version != merkleFormatVersion {
		return [32]byte{}, nil, fmt.Errorf("unsupported merkle format version %d in %s", version, path)
	}

	leafCountU, err := readUvarintAt(payload, &off)
	if err != nil {
		return [32]byte{}, nil, err
	}
	leafCount, err := checkedChunkLen(leafCountU, 1<<30, "merkle leaf count")
	if err != nil {
		return [32]byte{}, nil, err
	}

	if off+32 > len(payload) {
		return [32]byte{}, nil, fmt.Errorf("merkle root out of bounds in %s", path)
	}
	var root [32]byte
	copy(root[:], payload[off:off+32])
	off += 32

	if off+leafCount*32 > len(payload) {
		return [32]byte{}, nil, fmt.Errorf("merkle leaves out of bounds in %s", path)
	}
	leaves := make([][32]byte, leafCount)
	for i := 0; i < leafCount; i++ {
		copy(leaves[i][:], payload[off:off+32])
		off += 32
	}
	if off != len(payload) {
		return [32]byte{}, nil, fmt.Errorf("merkle trailing bytes in %s", path)
	}
	return root, leaves, nil
}

func (m *Manager) readAllDataValues(dataPath string) ([][]byte, error) {
	hdr, err := m.readFileHeader(dataPath)
	if err != nil {
		return nil, err
	}
	if hdr.magic != m.dataMagic {
		return nil, fmt.Errorf("invalid data magic in %s", dataPath)
	}

	blockCount, err := m.countBlocks(dataPath, hdr.blockSize)
	if err != nil {
		return nil, err
	}

	prevKey := ""
	var pending []byte
	values := make([][]byte, 0, blockCount)

	for blockNo := uint64(0); blockNo < blockCount; blockNo++ {
		payload, err := m.readPayloadBlock(dataPath, hdr.blockSize, blockNo)
		if err != nil {
			return nil, err
		}

		off := 0
		if blockNo == 0 {
			if len(payload) < 8 {
				return nil, fmt.Errorf("data header too short in %s", dataPath)
			}
			if string(payload[:4]) != string(m.dataMagic[:]) {
				return nil, fmt.Errorf("invalid data payload magic in %s", dataPath)
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
					values = append(values, append([]byte(nil), rec.Value...))

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
				values = append(values, append([]byte(nil), rec.Value...))
			}
		}
	}

	if len(pending) != 0 {
		return nil, fmt.Errorf("unterminated fragmented record in %s", dataPath)
	}
	return values, nil
}

func buildMerkleRoot(leaves [][32]byte) [32]byte {
	if len(leaves) == 0 {
		return sha256.Sum256(nil)
	}
	level := append([][32]byte(nil), leaves...)
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			var pair [64]byte
			copy(pair[0:32], left[:])
			copy(pair[32:64], right[:])
			next = append(next, sha256.Sum256(pair[:]))
		}
		level = next
	}
	return level[0]
}

func diffLeafHashes(expected, actual [][32]byte) []int {
	min := len(expected)
	if len(actual) < min {
		min = len(actual)
	}
	changed := make([]int, 0)
	for i := 0; i < min; i++ {
		if expected[i] != actual[i] {
			changed = append(changed, i)
		}
	}
	for i := min; i < len(expected); i++ {
		changed = append(changed, i)
	}
	for i := min; i < len(actual); i++ {
		changed = append(changed, i)
	}
	return changed
}

func (m *Manager) resolveSSTBasePath(table string) (string, error) {
	name := strings.TrimSpace(table)
	if name == "" {
		return "", fmt.Errorf("sstable name is empty")
	}
	for _, ext := range []string{".data", ".index", ".summary", ".filter", ".merkle"} {
		if strings.HasSuffix(name, ext) {
			name = strings.TrimSuffix(name, ext)
			break
		}
	}
	if filepath.IsAbs(name) {
		return name, nil
	}
	return filepath.Join(m.dir, name), nil
}
