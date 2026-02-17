package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"kv-engine/internal/block"
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
	cfg config.Config
	bm  *block.BlockManager
	wal *wal.WALManager
	mem memtable.MemtableManagerIface
	sst sstable.ManagerIface
	lsm *lsm.LSMTree
	seq uint64
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

	bm := block.NewBlockManager(cfg.CacheSize)

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

	e := &Engine{
		cfg: cfg,
		bm:  bm,
		mem: mem,
		sst: sstMgr,
		lsm: lsmTree,
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

	return e.ApplyRecord(rec, false)
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
	return e.sst.AppendProbMeta(meta)
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
	return e.sst.AppendProbMeta(meta)
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
	return e.sst.AppendProbMeta(meta)
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
	return e.sst.AppendProbMeta(meta)
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
	return e.sst.AppendProbMeta(meta)
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
	return e.sst.AppendProbMeta(meta)
}

func (e *Engine) BFGet(key string, value []byte) (bool, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeBloomFilter, key)
	if err != nil {
		return false, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		return false, nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeBloomFilter, key)
	if err != nil {
		return false, err
	}

	bloom, err := bloom.Merge(ops, int(meta.BFExpectedElements), float64(meta.BFFalsePositiveRate))
	if err != nil {
		return false, err
	}
	return bloom.MightContain(value), nil
}

func (e *Engine) CMSGet(key string, value []byte) (uint64, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeCountMinSketch, key)
	if err != nil {
		return 0, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		return 0, nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeCountMinSketch, key)
	if err != nil {
		return 0, err
	}

	cms := cms.Merge(ops, meta.CMSEpsilon, meta.CMSDelta)
	return cms.Estimate(value), nil
}

func (e *Engine) HLLGet(key string) (uint64, error) {
	meta, found, err := e.sst.GetLatestProbMeta(model.StructureTypeHyperLogLog, key)
	if err != nil {
		return 0, err
	}
	if !found || meta.Action != model.ProbMetaActionCreate {
		return 0, nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeHyperLogLog, key)
	if err != nil {
		return 0, err
	}

	hll := hll.Merge(ops, meta.HLLPrecision, meta.HLLSeed)
	return uint64(hll.Estimate()), nil
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
	now := uint64(time.Now().Unix())

	// 1) Memtable
	r := e.mem.Get(key)
	if r.Found {
		if r.Tombstone || (r.ExpiresAt > 0 && r.ExpiresAt <= now) {
			return nil, false, nil
		}
		return r.Value, true, nil
	}

	// 2) SSTable (all levels via LSM tree)
	val, found, err := e.lsm.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
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
		return e.flushMemtable()
	}
	return nil
}
