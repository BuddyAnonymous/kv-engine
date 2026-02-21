package sstable

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"kv-engine/internal/model"
)

const merkleFormatVersion = 1
// Write whole merkle file
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
// Validates merkle root of the given SSTable, returning validation result with details
func (m *Manager) ValidateMerkle(table string) (model.MerkleValidationResult, error) {
	basePath, err := m.resolveSSTBasePath(table)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	mode, err := m.resolveTableMode(basePath)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	switch mode {
	case tocModeSingle:
		return m.validateSingleFileMerkle(basePath)
	case tocModeMulti:
		return m.validateMultiFileMerkle(basePath)
	default:
		return model.MerkleValidationResult{}, fmt.Errorf("unsupported sstable mode %d for %s", mode, basePath)
	}
}
// Reads merkle file and returns expected root and leaf hashes
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
	return m.parseMerklePayload(payload, path)
}

// Reads all data values from data file, used for merkle validation
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
// Returns roothash from leaves
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
// Returns indexes of leaf hashes that differ between expected and actual
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
// Na osnovu sstable imena vraca putanju do sstable
func (m *Manager) resolveSSTBasePath(table string) (string, error) {
	name := strings.TrimSpace(table)
	if name == "" {
		return "", fmt.Errorf("sstable name is empty")
	}
	for _, ext := range []string{".data", ".index", ".summary", ".filter", ".merkle", ".sst", ".toc"} {
		if strings.HasSuffix(name, ext) {
			name = strings.TrimSuffix(name, ext)
			break
		}
	}
	if filepath.IsAbs(name) {
		return name, nil
	}

	rootDir := filepath.Dir(m.dir)
	candidates := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	addCandidate := func(p string) {
		cp := filepath.Clean(p)
		if _, ok := seen[cp]; ok {
			return
		}
		seen[cp] = struct{}{}
		candidates = append(candidates, cp)
	}

	// Try user-provided relative path as-is (from process cwd), manager dir,
	// and SSTable root dir.
	addCandidate(name)
	addCandidate(filepath.Join(m.dir, name))
	addCandidate(filepath.Join(rootDir, name))

	// If only base name is provided, also search all level* directories.
	if filepath.Base(name) == name {
		levelDirs, err := filepath.Glob(filepath.Join(rootDir, "level*"))
		if err != nil {
			return "", err
		}
		for _, dir := range levelDirs {
			addCandidate(filepath.Join(dir, name))
		}
	}

	matches := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ok, err := hasSSTableArtifacts(c)
		if err != nil {
			return "", err
		}
		if ok {
			matches = append(matches, c)
		}
	}

	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		sort.Strings(matches)
		return "", fmt.Errorf("ambiguous sstable name %q, matches: %s", table, strings.Join(matches, ", "))
	}

	// Keep old behavior fallback for error reporting in resolveTableMode.
	return filepath.Join(m.dir, name), nil
}
// Checks if any SSTable artifact files exist for the given base path
func hasSSTableArtifacts(basePath string) (bool, error) {
	for _, ext := range []string{".toc", ".sst", ".data"} {
		p := basePath + ext
		_, err := os.Stat(p)
		if err == nil {
			return true, nil
		}
		if !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}
// Validates multi file merkle
func (m *Manager) validateMultiFileMerkle(basePath string) (model.MerkleValidationResult, error) {
	expRoot, expLeaves, err := m.readMerkleFile(basePath + ".merkle")
	if err != nil {
		return model.MerkleValidationResult{}, err
	}
	values, err := m.readAllDataValues(basePath + ".data")
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	return buildMerkleValidationResult(values, expRoot, expLeaves), nil
}
// Validates single file merkle
func (m *Manager) validateSingleFileMerkle(basePath string) (model.MerkleValidationResult, error) {
	singlePath := basePath + ".sst"
	footer, blockSize, err := m.readSingleFooter(singlePath)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	expRoot, expLeaves, err := m.readMerkleFromSingleSection(singlePath, footer, blockSize)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}
	values, err := m.readAllSingleDataValues(singlePath, footer, blockSize)
	if err != nil {
		return model.MerkleValidationResult{}, err
	}

	return buildMerkleValidationResult(values, expRoot, expLeaves), nil
}
// Returns merkleValidationResult with details about validation, including expected vs actual root, leaf count, and indexes of changed leaves
func buildMerkleValidationResult(values [][]byte, expRoot [32]byte, expLeaves [][32]byte) model.MerkleValidationResult {
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
	}
}
// Returns tocMode (single / multi file)
func (m *Manager) resolveTableMode(basePath string) (uint64, error) {
	tocPath := basePath + ".toc"
	if _, err := os.Stat(tocPath); err == nil {
		return m.readTOCMode(tocPath)
	} else if !os.IsNotExist(err) {
		return 0, err
	}

	if _, err := os.Stat(basePath + ".sst"); err == nil {
		return tocModeSingle, nil
	} else if !os.IsNotExist(err) {
		return 0, err
	}
	if _, err := os.Stat(basePath + ".data"); err == nil {
		return tocModeMulti, nil
	} else if !os.IsNotExist(err) {
		return 0, err
	}

	return 0, fmt.Errorf("sstable not found for base path %s", basePath)
}
// Reads merkle section from single file
func (m *Manager) readMerkleFromSingleSection(singlePath string, footer singleFileFooter, blockSize int) ([32]byte, [][32]byte, error) {
	if footer.MerkleLen == 0 {
		return [32]byte{}, nil, fmt.Errorf("single file has empty merkle section: %s", singlePath)
	}

	payload := make([]byte, 0, int(footer.MerkleLen)*blockSize)
	for i := uint64(0); i < footer.MerkleLen; i++ {
		chunk, err := m.readPayloadBlock(singlePath, blockSize, footer.MerkleOffset+i)
		if err != nil {
			return [32]byte{}, nil, err
		}
		payload = append(payload, chunk...)
	}

	return m.parseMerklePayload(payload, singlePath)
}
// Reads all data values from single file, used for merkle validation
func (m *Manager) readAllSingleDataValues(singlePath string, footer singleFileFooter, blockSize int) ([][]byte, error) {
	values := make([][]byte, 0)
	err := m.scanSingleDataSection(singlePath, blockSize, footer, func(rec model.Record) (bool, error) {
		values = append(values, append([]byte(nil), rec.Value...))
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}
// Returns roothash and leaf hashes from merkle payload
func (m *Manager) parseMerklePayload(payload []byte, path string) ([32]byte, [][32]byte, error) {
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
