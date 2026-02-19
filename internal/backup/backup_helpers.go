package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func validateManifest(mf Manifest) error {
	if mf.Version != formatVersion {
		return fmt.Errorf("unsupported manifest version: %d", mf.Version)
	}
	if strings.TrimSpace(mf.ID) == "" {
		return fmt.Errorf("manifest id is empty")
	}
	if mf.Type != BackupTypeFull && mf.Type != BackupTypeIncremental {
		return fmt.Errorf("invalid backup type: %q", mf.Type)
	}
	if mf.Type == BackupTypeFull && strings.TrimSpace(mf.ParentID) != "" {
		return fmt.Errorf("full backup cannot have parent")
	}
	for _, f := range mf.Files {
		if err := validateRelPath(f.Path); err != nil {
			return err
		}
	}
	for _, p := range mf.Deleted {
		if err := validateRelPath(p); err != nil {
			return err
		}
	}
	return nil
}

func encodeManifestBinary(mf Manifest) ([]byte, error) {
	typeCode, err := backupTypeToByte(mf.Type)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write([]byte{'B', 'K', 'M', 'F'})
	if err := binary.Write(&buf, binary.LittleEndian, uint16(mf.Version)); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(typeCode); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, mf.CreatedAt.UnixNano()); err != nil {
		return nil, err
	}
	if err := writeString(&buf, mf.ID); err != nil {
		return nil, err
	}
	if err := writeString(&buf, mf.ParentID); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(mf.Files))); err != nil {
		return nil, err
	}
	for _, f := range mf.Files {
		if err := writeString(&buf, f.Path); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, f.Size); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, f.ModTimeUnixNano); err != nil {
			return nil, err
		}
		if _, err := buf.Write(f.SHA256[:]); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(mf.Deleted))); err != nil {
		return nil, err
	}
	for _, d := range mf.Deleted {
		if err := writeString(&buf, d); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeManifestBinary(b []byte) (Manifest, error) {
	off := 0
	readN := func(n int) ([]byte, error) {
		if off+n > len(b) {
			return nil, fmt.Errorf("manifest truncated")
		}
		ch := b[off : off+n]
		off += n
		return ch, nil
	}

	magic, err := readN(4)
	if err != nil {
		return Manifest{}, err
	}
	if string(magic) != "BKMF" {
		return Manifest{}, fmt.Errorf("invalid manifest magic")
	}

	verBytes, err := readN(2)
	if err != nil {
		return Manifest{}, err
	}
	version := int(binary.LittleEndian.Uint16(verBytes))

	typeRaw, err := readN(1)
	if err != nil {
		return Manifest{}, err
	}
	typ, err := backupTypeFromByte(typeRaw[0])
	if err != nil {
		return Manifest{}, err
	}

	tsBytes, err := readN(8)
	if err != nil {
		return Manifest{}, err
	}
	createdAtNano := int64(binary.LittleEndian.Uint64(tsBytes))

	id, err := readString(b, &off)
	if err != nil {
		return Manifest{}, err
	}
	parentID, err := readString(b, &off)
	if err != nil {
		return Manifest{}, err
	}

	filesCountBytes, err := readN(4)
	if err != nil {
		return Manifest{}, err
	}
	filesCount := binary.LittleEndian.Uint32(filesCountBytes)
	if filesCount > 1<<20 {
		return Manifest{}, fmt.Errorf("invalid files count")
	}

	files := make([]FileEntry, 0, filesCount)
	for i := uint32(0); i < filesCount; i++ {
		path, err := readString(b, &off)
		if err != nil {
			return Manifest{}, err
		}
		sizeBytes, err := readN(8)
		if err != nil {
			return Manifest{}, err
		}
		modBytes, err := readN(8)
		if err != nil {
			return Manifest{}, err
		}
		hashBytes, err := readN(32)
		if err != nil {
			return Manifest{}, err
		}
		var hash [32]byte
		copy(hash[:], hashBytes)

		files = append(files, FileEntry{
			Path:            path,
			Size:            int64(binary.LittleEndian.Uint64(sizeBytes)),
			ModTimeUnixNano: int64(binary.LittleEndian.Uint64(modBytes)),
			SHA256:          hash,
		})
	}

	deletedCountBytes, err := readN(4)
	if err != nil {
		return Manifest{}, err
	}
	deletedCount := binary.LittleEndian.Uint32(deletedCountBytes)
	if deletedCount > 1<<20 {
		return Manifest{}, fmt.Errorf("invalid deleted count")
	}

	deleted := make([]string, 0, deletedCount)
	for i := uint32(0); i < deletedCount; i++ {
		s, err := readString(b, &off)
		if err != nil {
			return Manifest{}, err
		}
		deleted = append(deleted, s)
	}

	if off != len(b) {
		return Manifest{}, fmt.Errorf("invalid trailing bytes in manifest")
	}

	return Manifest{
		Version:   version,
		ID:        id,
		Type:      typ,
		CreatedAt: time.Unix(0, createdAtNano).UTC(),
		ParentID:  parentID,
		Files:     files,
		Deleted:   deleted,
	}, nil
}

func readString(b []byte, off *int) (string, error) {
	if *off+4 > len(b) {
		return "", fmt.Errorf("manifest truncated")
	}
	n := binary.LittleEndian.Uint32(b[*off : *off+4])
	*off += 4
	if int(n) < 0 || *off+int(n) > len(b) {
		return "", fmt.Errorf("manifest string out of bounds")
	}
	s := string(b[*off : *off+int(n)])
	*off += int(n)
	return s, nil
}

func writeString(buf *bytes.Buffer, s string) error {
	if len(s) > int(^uint32(0)) {
		return fmt.Errorf("string too long")
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := buf.WriteString(s)
	return err
}

func backupTypeToByte(t BackupType) (byte, error) {
	switch t {
	case BackupTypeFull:
		return 1, nil
	case BackupTypeIncremental:
		return 2, nil
	default:
		return 0, fmt.Errorf("invalid backup type: %q", t)
	}
}

func backupTypeFromByte(b byte) (BackupType, error) {
	switch b {
	case 1:
		return BackupTypeFull, nil
	case 2:
		return BackupTypeIncremental, nil
	default:
		return "", fmt.Errorf("invalid backup type code: %d", b)
	}
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func validateRelPath(rel string) error {
	if strings.TrimSpace(rel) == "" {
		return fmt.Errorf("path is empty")
	}
	clean := filepath.ToSlash(filepath.Clean(rel))
	if clean == "." || clean == "/" {
		return fmt.Errorf("path is invalid: %s", rel)
	}
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return fmt.Errorf("path traversal is not allowed: %s", rel)
	}
	if strings.HasPrefix(clean, "/") {
		return fmt.Errorf("absolute path is not allowed: %s", rel)
	}
	if strings.Contains(clean, `\`) {
		return fmt.Errorf("path must not contain backslashes: %s", rel)
	}
	return nil
}

func joinUnderRoot(root string, rel string) (string, error) {
	if err := validateRelPath(rel); err != nil {
		return "", err
	}
	full := filepath.Join(root, filepath.FromSlash(rel))
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	cleanFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	relCheck, err := filepath.Rel(cleanRoot, cleanFull)
	if err != nil {
		return "", err
	}
	relCheck = filepath.ToSlash(relCheck)
	if strings.HasPrefix(relCheck, "../") || relCheck == ".." {
		return "", fmt.Errorf("path escapes root: %s", rel)
	}
	return cleanFull, nil
}

func hashFile(path string) ([32]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [32]byte{}, err
	}
	sum := h.Sum(nil)
	var out [32]byte
	copy(out[:], sum)
	return out, nil
}

func (mgr *BackupManager) writeBytesViaBlockManager(path string, data []byte, perm fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if len(data) == 0 {
		if err := mgr.bm.TruncateFile(path, 0); err != nil {
			return err
		}
		return os.Chmod(path, perm)
	}

	numBlocks := (len(data) + mgr.blockSize - 1) / mgr.blockSize
	for i := 0; i < numBlocks; i++ {
		start := i * mgr.blockSize
		end := start + mgr.blockSize
		if end > len(data) {
			end = len(data)
		}
		blockData := make([]byte, mgr.blockSize)
		copy(blockData, data[start:end])
		if err := mgr.bm.WriteBlock(path, uint64(i), blockData, mgr.blockSize); err != nil {
			return err
		}
	}
	if err := mgr.bm.TruncateFile(path, int64(len(data))); err != nil {
		return err
	}
	return os.Chmod(path, perm)
}

func (mgr *BackupManager) readBytesViaBlockManager(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 0 {
		return nil, fmt.Errorf("invalid file size: %d", size)
	}
	if size == 0 {
		return []byte{}, nil
	}

	numBlocks := (size + int64(mgr.blockSize) - 1) / int64(mgr.blockSize)
	out := make([]byte, 0, int(numBlocks)*mgr.blockSize)
	for i := int64(0); i < numBlocks; i++ {
		blockData, err := mgr.bm.ReadBlock(path, uint64(i), mgr.blockSize)
		if err != nil {
			return nil, err
		}
		out = append(out, blockData...)
	}
	return out[:int(size)], nil
}

func validateCreateReq(dataDir string, backupRoot string) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("data dir is empty")
	}
	if strings.TrimSpace(backupRoot) == "" {
		return fmt.Errorf("backup root is empty")
	}
	info, err := os.Stat(dataDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("data dir is not a directory: %s", dataDir)
	}
	if err := os.MkdirAll(backupRoot, 0o755); err != nil {
		return err
	}
	return nil
}

// LoadManifest ucitava manifest backup-a sa diska
func (mgr *BackupManager) LoadManifest(backupRoot string, backupID string) (Manifest, error) {
	if strings.TrimSpace(backupRoot) == "" {
		return Manifest{}, fmt.Errorf("backup root is empty")
	}
	if strings.TrimSpace(backupID) == "" {
		return Manifest{}, fmt.Errorf("backup id is empty")
	}

	p := filepath.Join(backupRoot, backupID, manifestFileName)
	b, err := mgr.readBytesViaBlockManager(p)
	if err != nil {
		return Manifest{}, err
	}
	mf, err := decodeManifestBinary(b)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(mf); err != nil {
		return Manifest{}, fmt.Errorf("invalid manifest %s: %w", p, err)
	}
	return mf, nil
}

// Upisuje manifest backup-a na disk
func (mgr *BackupManager) writeManifest(backupDir string, mf Manifest) error {
	if err := validateManifest(mf); err != nil {
		return err
	}
	p := filepath.Join(backupDir, manifestFileName)
	b, err := encodeManifestBinary(mf)
	if err != nil {
		return err
	}
	return mgr.writeBytesViaBlockManager(p, b, 0o644)
}
