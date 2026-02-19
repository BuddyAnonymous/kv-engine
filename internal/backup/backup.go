package backup

import (
	"crypto/rand"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kv-engine/internal/block"
)

const (
	manifestFileName = "manifest.bin"
	payloadDirName   = "payload"
	formatVersion    = 1
)

type BackupType string

const (
	BackupTypeFull        BackupType = "full"
	BackupTypeIncremental BackupType = "incremental"
)

type FileEntry struct {
	Path            string
	Size            int64
	ModTimeUnixNano int64
	SHA256          [32]byte
}

type Manifest struct {
	Version   int
	ID        string
	Type      BackupType
	CreatedAt time.Time
	ParentID  string
	Files     []FileEntry
	Deleted   []string
}

type CreateFullRequest struct {
	DataDir    string
	BackupRoot string
	BackupID   string
}

type CreateIncrementalRequest struct {
	DataDir    string
	BackupRoot string
	BackupID   string
	ParentID   string
}

type RestoreRequest struct {
	BackupRoot  string
	BackupID    string
	TargetDir   string
	CleanTarget bool
}

type scannedFile struct {
	RelPath         string
	AbsPath         string
	Size            int64
	ModTimeUnixNano int64
	SHA256          [32]byte
}

type BackupManager struct {
	bm        *block.BlockManager
	blockSize int
}

// Konstruktor BackupManagera-a
func NewBackupManager(bm *block.BlockManager, blockSize int) (*BackupManager, error) {
	if bm == nil {
		return nil, fmt.Errorf("block manager is nil")
	}
	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size: %d", blockSize)
	}
	return &BackupManager{
		bm:        bm,
		blockSize: blockSize,
	}, nil
}

// CreateFull kreira puni backup
func (mgr *BackupManager) CreateFull(req CreateFullRequest) (Manifest, error) {
	if err := validateCreateReq(req.DataDir, req.BackupRoot); err != nil {
		return Manifest{}, err
	}

	id, err := resolveBackupID(req.BackupID)
	if err != nil {
		return Manifest{}, err
	}
	backupDir := filepath.Join(req.BackupRoot, id)
	payloadDir := filepath.Join(backupDir, payloadDirName)

	if err := os.MkdirAll(payloadDir, 0o755); err != nil {
		return Manifest{}, err
	}
	if exists, err := pathExists(filepath.Join(backupDir, manifestFileName)); err != nil {
		return Manifest{}, err
	} else if exists {
		return Manifest{}, fmt.Errorf("backup id already exists: %s", id)
	}

	files, err := scanFiles(req.DataDir)
	if err != nil {
		return Manifest{}, err
	}

	entries := make([]FileEntry, 0, len(files))
	for _, f := range files {
		dst := filepath.Join(payloadDir, filepath.FromSlash(f.RelPath))
		if err := copyFile(f.AbsPath, dst); err != nil {
			return Manifest{}, err
		}
		entries = append(entries, FileEntry{
			Path:            f.RelPath,
			Size:            f.Size,
			ModTimeUnixNano: f.ModTimeUnixNano,
			SHA256:          f.SHA256,
		})
	}

	manifest := Manifest{
		Version:   formatVersion,
		ID:        id,
		Type:      BackupTypeFull,
		CreatedAt: time.Now().UTC(),
		Files:     entries,
	}
	if err := mgr.writeManifest(backupDir, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// CreateIncremental kreira inkrementalni backup u odnosu na parent backup.
func (mgr *BackupManager) CreateIncremental(req CreateIncrementalRequest) (Manifest, error) {
	if err := validateCreateReq(req.DataDir, req.BackupRoot); err != nil {
		return Manifest{}, err
	}

	parentID := strings.TrimSpace(req.ParentID)
	if parentID == "" {
		latest, ok, err := mgr.LatestBackup(req.BackupRoot)
		if err != nil {
			return Manifest{}, err
		}
		if !ok {
			return Manifest{}, fmt.Errorf("cannot create incremental backup without existing parent backup")
		}
		parentID = latest.ID
	}
	if _, err := mgr.LoadManifest(req.BackupRoot, parentID); err != nil {
		return Manifest{}, fmt.Errorf("parent backup not found: %w", err)
	}

	id, err := resolveBackupID(req.BackupID)
	if err != nil {
		return Manifest{}, err
	}
	backupDir := filepath.Join(req.BackupRoot, id)
	payloadDir := filepath.Join(backupDir, payloadDirName)
	if err := os.MkdirAll(payloadDir, 0o755); err != nil {
		return Manifest{}, err
	}
	if exists, err := pathExists(filepath.Join(backupDir, manifestFileName)); err != nil {
		return Manifest{}, err
	} else if exists {
		return Manifest{}, fmt.Errorf("backup id already exists: %s", id)
	}

	parentState, err := mgr.effectiveState(req.BackupRoot, parentID)
	if err != nil {
		return Manifest{}, err
	}
	currentFiles, err := scanFiles(req.DataDir)
	if err != nil {
		return Manifest{}, err
	}

	currentByPath := make(map[string]scannedFile, len(currentFiles))
	for _, f := range currentFiles {
		currentByPath[f.RelPath] = f
	}

	changed := make([]FileEntry, 0)
	for _, f := range currentFiles {
		p, ok := parentState[f.RelPath]
		if ok && p.SHA256 == f.SHA256 {
			continue
		}
		dst := filepath.Join(payloadDir, filepath.FromSlash(f.RelPath))
		if err := copyFile(f.AbsPath, dst); err != nil {
			return Manifest{}, err
		}
		changed = append(changed, FileEntry{
			Path:            f.RelPath,
			Size:            f.Size,
			ModTimeUnixNano: f.ModTimeUnixNano,
			SHA256:          f.SHA256,
		})
	}

	deleted := make([]string, 0)
	for path := range parentState {
		if _, ok := currentByPath[path]; !ok {
			deleted = append(deleted, path)
		}
	}
	sort.Strings(deleted)

	manifest := Manifest{
		Version:   formatVersion,
		ID:        id,
		Type:      BackupTypeIncremental,
		CreatedAt: time.Now().UTC(),
		ParentID:  parentID,
		Files:     changed,
		Deleted:   deleted,
	}
	if err := mgr.writeManifest(backupDir, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Restore vraca stanje fajlova iz backup-a u radni direktorijum
func (mgr *BackupManager) Restore(req RestoreRequest) error {
	if strings.TrimSpace(req.BackupRoot) == "" {
		return fmt.Errorf("backup root is empty")
	}
	if strings.TrimSpace(req.BackupID) == "" {
		return fmt.Errorf("backup id is empty")
	}
	if strings.TrimSpace(req.TargetDir) == "" {
		return fmt.Errorf("target dir is empty")
	}

	chain, err := mgr.chainFromFull(req.BackupRoot, req.BackupID)
	if err != nil {
		return err
	}
	if len(chain) == 0 || chain[0].Type != BackupTypeFull {
		return fmt.Errorf("backup chain for %s does not start with full backup", req.BackupID)
	}

	if req.CleanTarget {
		if err := os.RemoveAll(req.TargetDir); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(req.TargetDir, 0o755); err != nil {
		return err
	}

	for _, mf := range chain {
		for _, rel := range mf.Deleted {
			abs, err := joinUnderRoot(req.TargetDir, rel)
			if err != nil {
				return err
			}
			if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		for _, f := range mf.Files {
			src, err := joinUnderRoot(filepath.Join(req.BackupRoot, mf.ID, payloadDirName), f.Path)
			if err != nil {
				return err
			}
			dst, err := joinUnderRoot(req.TargetDir, f.Path)
			if err != nil {
				return err
			}
			if err := copyFile(src, dst); err != nil {
				return err
			}
		}
	}
	return nil
}

// ListBackups vraca listu svih backup-a koji postoje u backup folderu
func (mgr *BackupManager) ListBackups(backupRoot string) ([]Manifest, error) {
	if strings.TrimSpace(backupRoot) == "" {
		return nil, fmt.Errorf("backup root is empty")
	}

	entries, err := os.ReadDir(backupRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return []Manifest{}, nil
		}
		return nil, err
	}

	out := make([]Manifest, 0)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		mf, err := mgr.LoadManifest(backupRoot, e.Name())
		if err != nil {
			continue
		}
		out = append(out, mf)
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// LatestBackup vraca manifest najnovijeg backup-a u backup folderu
func (mgr *BackupManager) LatestBackup(backupRoot string) (Manifest, bool, error) {
	list, err := mgr.ListBackups(backupRoot)
	if err != nil {
		return Manifest{}, false, err
	}
	if len(list) == 0 {
		return Manifest{}, false, nil
	}
	return list[len(list)-1], true, nil
}

func (mgr *BackupManager) chainFromFull(backupRoot string, backupID string) ([]Manifest, error) {
	seen := make(map[string]bool)
	cur := strings.TrimSpace(backupID)
	rev := make([]Manifest, 0)

	for {
		if cur == "" {
			return nil, fmt.Errorf("broken backup chain: empty backup id")
		}
		if seen[cur] {
			return nil, fmt.Errorf("backup chain cycle detected at %s", cur)
		}
		seen[cur] = true

		mf, err := mgr.LoadManifest(backupRoot, cur)
		if err != nil {
			return nil, err
		}
		rev = append(rev, mf)
		if mf.Type == BackupTypeFull {
			break
		}
		cur = strings.TrimSpace(mf.ParentID)
	}

	chain := make([]Manifest, 0, len(rev))
	for i := len(rev) - 1; i >= 0; i-- {
		chain = append(chain, rev[i])
	}
	return chain, nil
}

func (mgr *BackupManager) effectiveState(backupRoot string, backupID string) (map[string]FileEntry, error) {
	chain, err := mgr.chainFromFull(backupRoot, backupID)
	if err != nil {
		return nil, err
	}

	state := make(map[string]FileEntry)
	for _, mf := range chain {
		for _, p := range mf.Deleted {
			delete(state, p)
		}
		for _, f := range mf.Files {
			state[f.Path] = f
		}
	}
	return state, nil
}

func scanFiles(root string) ([]scannedFile, error) {
	out := make([]scannedFile, 0)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks are not supported in backup input: %s", path)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unsupported file type in backup input: %s", path)
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if err := validateRelPath(rel); err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		hash, err := hashFile(path)
		if err != nil {
			return err
		}

		out = append(out, scannedFile{
			RelPath:         rel,
			AbsPath:         path,
			Size:            info.Size(),
			ModTimeUnixNano: info.ModTime().UnixNano(),
			SHA256:          hash,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RelPath < out[j].RelPath
	})
	return out, nil
}

func copyFile(src string, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func resolveBackupID(preferred string) (string, error) {
	id := strings.TrimSpace(preferred)
	if id == "" {
		suffix := make([]byte, 4)
		if _, err := rand.Read(suffix); err != nil {
			return "", err
		}
		id = fmt.Sprintf("bkp_%s_%x", time.Now().UTC().Format("20060102_150405.000000000"), suffix)
	}
	if err := validateRelPath(id); err != nil {
		return "", fmt.Errorf("invalid backup id: %w", err)
	}
	return id, nil
}
