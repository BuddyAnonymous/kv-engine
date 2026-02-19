package engine

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kv-engine/internal/backup"
	"kv-engine/internal/block"
	"kv-engine/internal/cache"
	"kv-engine/internal/config"
	"kv-engine/internal/lsm"
	"kv-engine/internal/memtable"
	"kv-engine/internal/model"
	"kv-engine/internal/sstable"
	"kv-engine/internal/wal"

	"kv-engine/internal/probabilistic/bloom"
	"kv-engine/internal/probabilistic/cms"
	"kv-engine/internal/probabilistic/hll"
)

type Engine struct {
	cfg        config.Config
	bm         *block.BlockManager
	cache      *cache.Cache
	wal        *wal.WALManager
	mem        memtable.MemtableManagerIface
	sst        sstable.ManagerIface
	lsm        *lsm.LSMTree
	backupMgr  *backup.BackupManager
	seq        uint64
	cacheEpoch uint64
}

func New(cfg config.Config) (*Engine, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	fact, err := memtable.FactoryFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	mem, err := memtable.NewMemtableManager(cfg.MemtableInstances, fact)
	if err != nil {
		return nil, err
	}

	bm := block.NewBlockManager(cfg.BlockCacheSize)

	sstBaseDir := filepath.Join(cfg.DataDir, "sstable")
	sstMgr := sstable.New(filepath.Join(sstBaseDir, "level0"), cfg.MultiFileSSTable, bm, cfg.BlockSize, uint64(cfg.SummaryStride))

	lsmCfg := lsm.LSMConfig{
		MaxLevels:             cfg.LSMMaxLevels,
		Algorithm:             cfg.LSMCompactionAlgorithm,
		SizeTieredMinSSTables: cfg.LSMSizeTieredMinSSTables,
		LeveledL0Threshold:    cfg.LSMLeveledL0Threshold,
		LeveledBaseSizeMB:     cfg.LSMLeveledBaseSizeMB,
		LeveledMultiplier:     cfg.LSMLeveledMultiplier,
	}

	lsmTree, err := lsm.NewLSMTree(lsmCfg, sstMgr, sstBaseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create lsm tree: %w", err)
	}

	backupMgr, err := backup.NewBackupManager(bm, cfg.BlockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup manager: %w", err)
	}

	e := &Engine{
		cfg:        cfg,
		bm:         bm,
		cache:      cache.New(cfg.CacheSize),
		mem:        mem,
		sst:        sstMgr,
		lsm:        lsmTree,
		backupMgr:  backupMgr,
		cacheEpoch: 1,
	}

	// Inicijalizuj WAL sa engine-om kao applier (replay se desava unutar NewWALManager)
	walManager, walErr, lastSeq := wal.NewWALManager(filepath.Join(cfg.DataDir, "wal"), cfg.SegmentBlocks, cfg.BlockSize, bm, e)
	if walErr != nil {
		return nil, walErr
	}
	e.wal = walManager
	if lastSeq > e.seq {
		e.seq = lastSeq
	}

	maxProbMetaSeq, err := e.sst.MaxProbMetaSeq()
	if err != nil {
		return nil, err
	}
	if maxProbMetaSeq > e.seq {
		e.seq = maxProbMetaSeq
	}

	return e, nil
}

func (e *Engine) Put(key string, value []byte, ttl ...time.Duration) error {
	e.seq++
	var expiresAt uint64
	if len(ttl) > 0 {
		expiresAt = uint64(time.Now().Add(ttl[0]).Unix())
	}
	rec := model.Record{
		Key:       key,
		Value:     value,
		Tombstone: false,
		Seq:       e.seq,
		ExpiresAt: expiresAt,
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}

	return e.ApplyRecord(rec, false)
}

func (e *Engine) Merge(structure model.StructureType, key string, value []byte, op model.MergeOpType, ttl ...time.Duration) error {
	if structure == model.StructureTypeNone {
		return fmt.Errorf("invalid merge structure type")
	}
	if op != model.MergeOpAdd {
		return fmt.Errorf("invalid merge op type")
	}
	meta, found, err := e.sst.GetLatestProbMeta(structure, key)
	if err != nil {
		return err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		return fmt.Errorf("%s instance is not created for key=%s", structureName(structure), key)
	}

	e.seq++
	var expiresAt uint64
	if len(ttl) > 0 {
		expiresAt = uint64(time.Now().Add(ttl[0]).Unix())
	}
	rec := model.Record{
		Key:       key,
		Value:     value,
		Tombstone: false,
		Seq:       e.seq,
		ExpiresAt: expiresAt,
		Kind:      model.RecordKindMergeOperand,
		Structure: structure,
		Op:        op,
	}

	if err := e.ApplyRecord(rec, false); err != nil {
		return err
	}
	return nil
}

func (e *Engine) BFAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeBloomFilter, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) BFCreate(key string) error {
	if key == "" {
		return fmt.Errorf("bf key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure:           model.StructureTypeBloomFilter,
		Key:                 key,
		Action:              model.ProbMetaActionCreate,
		Seq:                 e.seq,
		BFExpectedElements:  e.cfg.BFExpectedElements,
		BFFalsePositiveRate: e.cfg.BFFalsePositiveRate,
		BFSeed:              e.cfg.BFSeed,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeBloomFilter, key)
	return nil
}

func (e *Engine) BFDelete(key string) error {
	if key == "" {
		return fmt.Errorf("bf key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure: model.StructureTypeBloomFilter,
		Key:       key,
		Action:    model.ProbMetaActionDelete,
		Seq:       e.seq,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeBloomFilter, key)
	return nil
}

func (e *Engine) CMSAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeCountMinSketch, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) CMSCreate(key string) error {
	if key == "" {
		return fmt.Errorf("cms key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure:  model.StructureTypeCountMinSketch,
		Key:        key,
		Action:     model.ProbMetaActionCreate,
		Seq:        e.seq,
		CMSEpsilon: e.cfg.CMSEpsilon,
		CMSDelta:   e.cfg.CMSDelta,
		CMSSeed:    e.cfg.CMSSeed,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeCountMinSketch, key)
	return nil
}

func (e *Engine) CMSDelete(key string) error {
	if key == "" {
		return fmt.Errorf("cms key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure: model.StructureTypeCountMinSketch,
		Key:       key,
		Action:    model.ProbMetaActionDelete,
		Seq:       e.seq,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeCountMinSketch, key)
	return nil
}

func (e *Engine) HLLAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeHyperLogLog, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) HLLCreate(key string) error {
	if key == "" {
		return fmt.Errorf("hll key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure:    model.StructureTypeHyperLogLog,
		Key:          key,
		Action:       model.ProbMetaActionCreate,
		Seq:          e.seq,
		HLLPrecision: e.cfg.HLLPrecision,
		HLLSeed:      e.cfg.HLLSeed,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeHyperLogLog, key)
	return nil
}

func (e *Engine) HLLDelete(key string) error {
	if key == "" {
		return fmt.Errorf("hll key is empty")
	}
	e.seq++
	meta := model.ProbMetaRecord{
		Structure: model.StructureTypeHyperLogLog,
		Key:       key,
		Action:    model.ProbMetaActionDelete,
		Seq:       e.seq,
	}
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeHyperLogLog, key)
	return nil
}

func (e *Engine) BFGet(key string, value []byte) (bool, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeBloomFilter, key)
	if err != nil {
		return false, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		e.invalidateStructureCache(model.StructureTypeBloomFilter, key)
		return false, nil
	}
	if bf, ok := e.getBloomFromCache(key); ok {
		return bf.MightContain(value), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeBloomFilter, key)
	if err != nil {
		return false, err
	}

	bf, err := bloom.Merge(ops, int(meta.BFExpectedElements), float64(meta.BFFalsePositiveRate))
	if err != nil {
		return false, err
	}
	e.putBloomToCache(key, bf, maxSeq(meta.Seq, maxSeqFromRecords(ops)))
	return bf.MightContain(value), nil
}

func (e *Engine) CMSGet(key string, value []byte) (uint64, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeCountMinSketch, key)
	if err != nil {
		return 0, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		e.invalidateStructureCache(model.StructureTypeCountMinSketch, key)
		return 0, nil
	}
	if sketch, ok := e.getCMSFromCache(key); ok {
		return sketch.Estimate(value), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeCountMinSketch, key)
	if err != nil {
		return 0, err
	}

	sketch := cms.Merge(ops, meta.CMSEpsilon, meta.CMSDelta)
	e.putCMSToCache(key, sketch, maxSeq(meta.Seq, maxSeqFromRecords(ops)))
	return sketch.Estimate(value), nil
}

func (e *Engine) HLLGet(key string) (uint64, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeHyperLogLog, key)
	if err != nil {
		return 0, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		e.invalidateStructureCache(model.StructureTypeHyperLogLog, key)
		return 0, nil
	}
	if structure, ok := e.getHLLFromCache(key); ok {
		return uint64(structure.Estimate()), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeHyperLogLog, key)
	if err != nil {
		return 0, err
	}

	structure := hll.Merge(ops, meta.HLLPrecision, meta.HLLSeed)
	e.putHLLToCache(key, structure, maxSeq(meta.Seq, maxSeqFromRecords(ops)))
	return uint64(structure.Estimate()), nil
}

func (e *Engine) Delete(key string) error {
	e.seq++
	rec := model.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
		Seq:       e.seq,
		ExpiresAt: 0,
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}

	return e.ApplyRecord(rec, false)
}

func (e *Engine) Get(key string) ([]byte, bool, error) {
	if val, ok := e.getKVFromCache(key); ok {
		return val, true, nil
	}

	now := uint64(time.Now().Unix())

	// 1) Memtable
	r := e.mem.Get(key)
	if r.Found {
		if r.Tombstone || (r.ExpiresAt > 0 && r.ExpiresAt <= now) {
			e.invalidateKVCache(key)
			return nil, false, nil
		}
		e.putKVToCache(key, r.Value, r.Seq, r.ExpiresAt)
		return r.Value, true, nil
	}

	// 2) SSTable (all levels via LSM tree)
	val, found, err := e.lsm.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		e.invalidateKVCache(key)
		return nil, false, nil
	}
	e.putKVToCache(key, val, 0, 0)
	return val, true, nil
}

func (e *Engine) ValidateMerkle(table string) (model.MerkleValidationResult, error) {
	return e.sst.ValidateMerkle(table)
}

func (e *Engine) flushMemtable() error {
	records, ok := e.mem.NextFlushBatch()
	if !ok {
		return nil
	}
	if err := e.lsm.Flush(records); err != nil {
		return err
	}
	e.bumpCacheEpoch()

	// During startup WAL replay e.wal is not assigned yet. In normal runtime,
	// after a successful flush we can safely drop fully persisted WAL segments.
	if e.wal != nil {
		var maxSeq uint64
		for _, rec := range records {
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
		}
		if maxSeq > 0 {
			if err := e.wal.CheckWAL(maxSeq); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Engine) getAllMergeOperands(structure model.StructureType, key string) ([]model.Record, error) {
	now := uint64(time.Now().Unix())
	ops := make([]model.Record, 0)

	memOps := e.mem.GetMergeOperands(structure, key)
	for _, rec := range memOps {
		if rec.Kind != model.RecordKindMergeOperand {
			continue
		}
		if rec.Structure != structure {
			continue
		}
		if rec.Op != model.MergeOpAdd {
			continue
		}
		if rec.ExpiresAt > 0 && rec.ExpiresAt <= now {
			continue
		}
		ops = append(ops, rec)
	}

	sstOps, err := e.lsm.GetMergeOperands(structure, key)
	if err != nil {
		return nil, err
	}
	ops = append(ops, sstOps...)

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

func maxSeqFromRecords(records []model.Record) uint64 {
	var out uint64
	for _, rec := range records {
		if rec.Seq > out {
			out = rec.Seq
		}
	}
	return out
}

func maxSeq(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func structureName(s model.StructureType) string {
	switch s {
	case model.StructureTypeBloomFilter:
		return "bloom_filter"
	case model.StructureTypeCountMinSketch:
		return "count_min_sketch"
	case model.StructureTypeHyperLogLog:
		return "hyper_log_log"
	default:
		return "unknown_structure"
	}
}

// applyRecord upisuje record u memtable, a u WAL samo ako zapis nije stigao iz replay-a.
func (e *Engine) ApplyRecord(rec model.Record, fromWAL bool) error {
	if rec.Seq > e.seq {
		e.seq = rec.Seq
	}

	if !fromWAL {
		if err := e.wal.Append(rec); err != nil {
			return err
		}
	}

	var (
		flushNeeded bool
		err         error
	)
	if rec.Kind == model.RecordKindKV && rec.Tombstone {
		flushNeeded, err = e.mem.Delete(rec)
	} else {
		flushNeeded, err = e.mem.Put(rec)
	}
	if err != nil {
		return err
	}
	if flushNeeded {
		if err := e.flushMemtable(); err != nil {
			return err
		}
	}

	if !fromWAL {
		if rec.Kind == model.RecordKindKV {
			if rec.Tombstone || (rec.ExpiresAt > 0 && rec.ExpiresAt <= uint64(time.Now().Unix())) {
				e.invalidateKVCache(rec.Key)
			} else {
				e.putKVToCache(rec.Key, rec.Value, rec.Seq, rec.ExpiresAt)
			}
		} else {
			if !e.updateStructureCacheOnMergeAdd(rec) {
				e.invalidateStructureCache(rec.Structure, rec.Key)
			}
		}
	}

	return nil
}

// Backup pokrece interaktivni meni za backup i restore operacije nad engine-om.
func (e *Engine) Backup() error {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\n=== Key-Value Backup Menu ===")
		fmt.Println("1) Napravi full backup")
		fmt.Println("2) Napravi inkrementalni backup")
		fmt.Println("3) Restore backupa")
		fmt.Println("4) Lista dostupnih backup-a")
		fmt.Println("0) Izlaz")
		fmt.Print("Izaberi opciju: ")

		if !scanner.Scan() {
			break
		}
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			fmt.Print("Backup ID (ostavi prazno za automatski): ")
			scanner.Scan()
			id := strings.TrimSpace(scanner.Text())
			mf, err := e.backupMgr.CreateFull(backup.CreateFullRequest{
				DataDir:    e.cfg.DataDir,
				BackupRoot: e.cfg.BackupRoot,
				BackupID:   id,
			})
			if err != nil {
				fmt.Println("greska pri full backup-u:", err)
				continue
			}
			fmt.Printf("Full backup kreiran: ID=%s, fajlova=%d\n", mf.ID, len(mf.Files))

		case "2":
			fmt.Print("Parent backup ID (ostavi prazno za poslednji): ")
			scanner.Scan()
			parentID := strings.TrimSpace(scanner.Text())

			fmt.Print("Backup ID (ostavi prazno za automatski): ")
			scanner.Scan()
			id := strings.TrimSpace(scanner.Text())

			mf, err := e.backupMgr.CreateIncremental(backup.CreateIncrementalRequest{
				DataDir:    e.cfg.DataDir,
				BackupRoot: e.cfg.BackupRoot,
				BackupID:   id,
				ParentID:   parentID,
			})
			if err != nil {
				fmt.Println("greska pri inkrementalnom backup-u:", err)
				continue
			}
			fmt.Printf("Inkrementalni backup kreiran: ID=%s, izmenjeno=%d, obrisano=%d\n", mf.ID, len(mf.Files), len(mf.Deleted))

		case "3":
			fmt.Print("Backup ID za restore: ")
			scanner.Scan()
			id := strings.TrimSpace(scanner.Text())
			if id == "" {
				fmt.Println("backup ID je obavezan")
				continue
			}

			fmt.Println("\n⚠ UPOZORENJE: Restore ce prepisati podatke u direktorijumu:", e.cfg.DataDir)
			fmt.Println("  Stari podaci ce biti trajno obrisani i zamenjeni sadrzajem backup-a.")
			fmt.Print("Da li ste sigurni da zelite da nastavite? (d/n): ")
			scanner.Scan()
			confirm := strings.ToLower(strings.TrimSpace(scanner.Text()))
			if confirm != "d" {
				fmt.Println("Restore otkazan.")
				continue
			}

			if err := e.backupMgr.Restore(backup.RestoreRequest{
				BackupRoot:  e.cfg.BackupRoot,
				BackupID:    id,
				TargetDir:   e.cfg.DataDir,
				CleanTarget: true,
			}); err != nil {
				fmt.Println("greska pri restore-u:", err)
				continue
			}
			fmt.Println("Restore zavrsen uspesno.")
			// Obrisati cache i memtable, i replay-ovati WAL da se podaci ucitaju u memtable nakon restore-a

		case "4":
			list, err := e.backupMgr.ListBackups(e.cfg.BackupRoot)
			if err != nil {
				fmt.Println("greska pri listanju:", err)
				continue
			}
			if len(list) == 0 {
				fmt.Println("Nema dostupnih backup-a.")
				continue
			}
			fmt.Printf("%-40s %-14s %-26s %-20s %s\n", "ID", "Tip", "Kreiran", "Parent", "Fajlova")
			for _, m := range list {
				parent := m.ParentID
				if parent == "" {
					parent = "-"
				}
				fmt.Printf("%-40s %-14s %-26s %-20s %d\n", m.ID, m.Type, m.CreatedAt.Format(time.RFC3339), parent, len(m.Files))
			}

		case "0":
			return nil

		default:
			fmt.Println("Nepoznata opcija.")
		}
	}
	return nil
}
