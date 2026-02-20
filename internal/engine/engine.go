package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"kv-engine/internal/block"
	"kv-engine/internal/config"
	"kv-engine/internal/memtable"
	"kv-engine/internal/model"
	"kv-engine/internal/ratelimit"
	"kv-engine/internal/sstable"
	"kv-engine/internal/wal"

	"kv-engine/internal/probabilistic/bloom"
	"kv-engine/internal/probabilistic/cms"
	"kv-engine/internal/probabilistic/hll"
)

type Engine struct {
	cfg     config.Config
	bm      *block.BlockManager
	wal     *wal.WALManager
	mem     memtable.MemtableManagerIface
	sst     sstable.ManagerIface
	rl      *ratelimit.TokenBucket
	seq     uint64
	history map[string][]model.Record

	snapshots   map[string]snapshotPoint
	snapshotCtr uint64
	stateMu     sync.Mutex

	// Test hooks (nil in production).
	testHookAfterTokenBucketSync func() error
	testHookAfterBatchBegin      func() error
	testHookAfterBatchSync       func() error
}

const (
	tokenBucketStateKey = "__sys__:token_bucket_state"
	internalKeyPrefix   = "__sys__:"
)

type KVPair struct {
	Key   string
	Value []byte
}

type snapshotPoint struct {
	Seq           uint64
	CreatedAtUnix uint64
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

	e := &Engine{
		cfg:       cfg,
		bm:        bm,
		mem:       mem,
		sst:       sstable.New(filepath.Join(cfg.DataDir, "sstable", "level0"), cfg.MultiFileSSTable, bm, cfg.BlockSize, uint64(cfg.SummaryStride)),
		history:   make(map[string][]model.Record),
		snapshots: make(map[string]snapshotPoint),
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

	if err := e.initTokenBucket(); err != nil {
		return nil, err
	}

	return e, nil
}

func isInternalKey(key string) bool {
	return strings.HasPrefix(key, internalKeyPrefix)
}

func (e *Engine) appendHistory(rec model.Record) {
	if rec.Key == "" {
		return
	}
	if isInternalKey(rec.Key) {
		return
	}
	e.history[rec.Key] = append(e.history[rec.Key], rec)
}

func (e *Engine) captureBaselineIfNeeded(key string, beforeSeq uint64) (model.Record, bool, error) {
	if key == "" || isInternalKey(key) {
		return model.Record{}, false, nil
	}
	if len(e.history[key]) > 0 {
		return model.Record{}, false, nil
	}
	val, found, err := e.getRaw(key)
	if err != nil {
		return model.Record{}, false, err
	}
	if found {
		return model.Record{
			Key:       key,
			Value:     append([]byte(nil), val...),
			Tombstone: false,
			Seq:       beforeSeq,
			ExpiresAt: 0,
			Kind:      model.RecordKindKV,
			Structure: model.StructureTypeNone,
			Op:        model.MergeOpNone,
		}, true, nil
	}
	// Baseline "none" marker as tombstone snapshot state.
	return model.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
		Seq:       beforeSeq,
		ExpiresAt: 0,
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}, true, nil
}

func (e *Engine) applyRecordToMem(rec model.Record) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

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

func (e *Engine) getRaw(key string) ([]byte, bool, error) {
	now := uint64(time.Now().Unix())
	r := e.mem.Get(key)
	if r.Found {
		if r.Tombstone || (r.ExpiresAt > 0 && r.ExpiresAt <= now) {
			return nil, false, nil
		}
		return r.Value, true, nil
	}
	return e.sst.Get(key)
}

func (e *Engine) initTokenBucket() error {
	capacity := e.cfg.TokenBucketTokens
	interval := time.Duration(e.cfg.TokenBucketInterval) * time.Millisecond
	bucket, err := ratelimit.New(capacity, interval, time.Now())
	if err != nil {
		return err
	}
	data, found, err := e.getRaw(tokenBucketStateKey)
	if err != nil {
		return err
	}
	if found {
		if err := bucket.UnmarshalBinary(data); err != nil {
			return err
		}
	}
	e.rl = bucket
	return nil
}

func (e *Engine) persistTokenBucketState() (bool, error) {
	if e.rl == nil {
		return false, nil
	}
	e.seq++
	rec := model.Record{
		Key:       tokenBucketStateKey,
		Value:     e.rl.MarshalBinary(),
		Tombstone: false,
		Seq:       e.seq,
		ExpiresAt: 0,
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}
	if err := e.wal.Append(rec); err != nil {
		return false, err
	}
	if err := e.wal.Sync(); err != nil {
		return false, err
	}
	if e.testHookAfterTokenBucketSync != nil {
		if err := e.testHookAfterTokenBucketSync(); err != nil {
			return true, err
		}
	}
	if err := e.applyRecordToMem(rec); err != nil {
		return true, err
	}
	return true, nil
}

func (e *Engine) allowRequest(cost int64) error {
	if e.rl == nil {
		return nil
	}
	snap := e.rl.Snapshot()
	if !e.rl.TryConsumeN(time.Now(), cost) {
		return fmt.Errorf("rate limit exceeded")
	}
	durable, err := e.persistTokenBucketState()
	if err != nil {
		if !durable {
			e.rl.Restore(snap)
		}
		return err
	}
	return nil
}

func (e *Engine) withRateLimitRollbackOnError(cost int64, op func() error) error {
	if e.rl == nil {
		return op()
	}
	snap := e.rl.Snapshot()
	if !e.rl.TryConsumeN(time.Now(), cost) {
		return fmt.Errorf("rate limit exceeded")
	}
	durable, err := e.persistTokenBucketState()
	if err != nil {
		if !durable {
			e.rl.Restore(snap)
		}
		return err
	}
	if err := op(); err != nil {
		e.rl.Restore(snap)
		if _, rbErr := e.persistTokenBucketState(); rbErr != nil {
			return fmt.Errorf("%v; token rollback failed: %w", err, rbErr)
		}
		return err
	}
	return nil
}

func (e *Engine) Put(key string, value []byte, ttl ...time.Duration) error {
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
		Kind:      model.RecordKindKV,
		Structure: model.StructureTypeNone,
		Op:        model.MergeOpNone,
	}

	return e.ApplyRecord(rec, false)
}

func (e *Engine) BatchWrite(pairs []KVPair, ttl ...time.Duration) error {
	if len(pairs) == 0 {
		return fmt.Errorf("batch is empty")
	}

	var expiresAt uint64
	if len(ttl) > 0 {
		expiresAt = uint64(time.Now().Add(ttl[0]).Unix())
	}

	records := make([]model.Record, 0, len(pairs))
	for _, p := range pairs {
		if strings.TrimSpace(p.Key) == "" {
			return fmt.Errorf("batch contains empty key")
		}
		if isInternalKey(p.Key) {
			return fmt.Errorf("internal key is not accessible")
		}
	}
	for _, p := range pairs {
		e.seq++
		records = append(records, model.Record{
			Key:       p.Key,
			Value:     p.Value,
			Tombstone: false,
			Seq:       e.seq,
			ExpiresAt: expiresAt,
			Kind:      model.RecordKindKV,
			Structure: model.StructureTypeNone,
			Op:        model.MergeOpNone,
		})
	}

	return e.withRateLimitRollbackOnError(int64(len(pairs)), func() error {
		return e.applyBatchRecords(records)
	})
}

func (e *Engine) Merge(structure model.StructureType, key string, value []byte, op model.MergeOpType, ttl ...time.Duration) error {
	if structure == model.StructureTypeNone {
		return fmt.Errorf("invalid merge structure type")
	}
	if op != model.MergeOpAdd {
		return fmt.Errorf("invalid merge op type")
	}
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
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
	if isInternalKey(key) {
		return false, fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return false, err
	}
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
	if isInternalKey(key) {
		return 0, fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return 0, err
	}
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
	if isInternalKey(key) {
		return 0, fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return 0, err
	}
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
	if isInternalKey(key) {
		return fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return err
	}
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

func (e *Engine) DeleteRange(startKey, endKey string) error {
	startKey = strings.TrimSpace(startKey)
	endKey = strings.TrimSpace(endKey)
	if startKey == "" || endKey == "" {
		return fmt.Errorf("range keys must not be empty")
	}
	if startKey > endKey {
		return fmt.Errorf("invalid range: start key must be <= end key")
	}

	memKeys, err := e.mem.ListLiveKeysInRange(startKey, endKey)
	if err != nil {
		return err
	}
	sstKeys, err := e.sst.ListLiveKeysInRange(startKey, endKey)
	if err != nil {
		return err
	}

	keySet := make(map[string]struct{}, len(memKeys)+len(sstKeys))
	for _, k := range memKeys {
		if isInternalKey(k) {
			continue
		}
		keySet[k] = struct{}{}
	}
	for _, k := range sstKeys {
		if isInternalKey(k) {
			continue
		}
		keySet[k] = struct{}{}
	}
	if len(keySet) == 0 {
		return e.allowRequest(1)
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	records := make([]model.Record, 0, len(keys))
	for _, key := range keys {
		e.seq++
		records = append(records, model.Record{
			Key:       key,
			Value:     nil,
			Tombstone: true,
			Seq:       e.seq,
			ExpiresAt: 0,
			Kind:      model.RecordKindKV,
			Structure: model.StructureTypeNone,
			Op:        model.MergeOpNone,
		})
	}
	return e.withRateLimitRollbackOnError(int64(len(keySet)), func() error {
		return e.applyBatchRecords(records)
	})
}

func (e *Engine) Get(key string) ([]byte, bool, error) {
	if isInternalKey(key) {
		return nil, false, fmt.Errorf("internal key is not accessible")
	}
	if err := e.allowRequest(1); err != nil {
		return nil, false, err
	}
	val, found, err := e.getRaw(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return val, true, nil
}

func (e *Engine) ValidateMerkle(table string) (model.MerkleValidationResult, error) {
	if err := e.allowRequest(1); err != nil {
		return model.MerkleValidationResult{}, err
	}
	return e.sst.ValidateMerkle(table)
}

func (e *Engine) SnapshotCreate(name ...string) (string, error) {
	id := ""
	if len(name) > 0 {
		id = strings.TrimSpace(name[0])
		if id == "" {
			return "", fmt.Errorf("snapshot name must not be empty")
		}
	} else {
		e.snapshotCtr++
		id = fmt.Sprintf("snap-%d", e.snapshotCtr)
	}
	if _, exists := e.snapshots[id]; exists {
		return "", fmt.Errorf("snapshot %s already exists", id)
	}
	if err := e.allowRequest(1); err != nil {
		return "", err
	}
	e.snapshots[id] = snapshotPoint{
		Seq:           e.seq,
		CreatedAtUnix: uint64(time.Now().Unix()),
	}
	return id, nil
}

func (e *Engine) SnapshotGet(snapshotID, key string) ([]byte, bool, error) {
	if isInternalKey(key) {
		return nil, false, fmt.Errorf("internal key is not accessible")
	}
	snap, ok := e.snapshots[snapshotID]
	if !ok {
		return nil, false, fmt.Errorf("snapshot %s not found", snapshotID)
	}
	if err := e.allowRequest(1); err != nil {
		return nil, false, err
	}
	h := e.history[key]
	for i := len(h) - 1; i >= 0; i-- {
		rec := h[i]
		if rec.Seq > snap.Seq {
			continue
		}
		if rec.Tombstone {
			return nil, false, nil
		}
		if rec.ExpiresAt > 0 && rec.ExpiresAt <= snap.CreatedAtUnix {
			return nil, false, nil
		}
		return rec.Value, true, nil
	}
	if len(h) == 0 {
		// Key has not changed in current process lifetime.
		return e.getRaw(key)
	}
	return nil, false, nil
}

func (e *Engine) CheckpointCreate(name ...string) (string, int, error) {
	cpName := ""
	if len(name) > 0 {
		cpName = strings.TrimSpace(name[0])
		if cpName == "" {
			return "", 0, fmt.Errorf("checkpoint name must not be empty")
		}
		if cpName != filepath.Base(cpName) {
			return "", 0, fmt.Errorf("checkpoint name must not contain path separators")
		}
	} else {
		cpName = fmt.Sprintf("checkpoint-%d", time.Now().UnixNano())
	}
	if err := e.allowRequest(1); err != nil {
		return "", 0, err
	}
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return "", 0, err
		}
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	if err := e.forceFlushAllMemtables(); err != nil {
		return "", 0, err
	}
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return "", 0, err
		}
	}

	checkpointsDir := filepath.Join(e.cfg.DataDir, "checkpoints")
	destRoot := filepath.Join(checkpointsDir, cpName)
	if _, err := os.Stat(destRoot); err == nil {
		return "", 0, fmt.Errorf("checkpoint %s already exists", cpName)
	}
	if err := os.MkdirAll(destRoot, 0755); err != nil {
		return "", 0, err
	}

	files, err := e.listCheckpointFiles()
	if err != nil {
		return "", 0, err
	}
	linked := 0
	for _, entry := range files {
		target := filepath.Join(destRoot, entry.relDest)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return "", 0, err
		}
		if err := os.Link(entry.src, target); err != nil {
			return "", 0, err
		}
		linked++
	}
	return destRoot, linked, nil
}

func (e *Engine) forceFlushAllMemtables() error {
	batches, err := e.mem.ForceFlushAll()
	if err != nil {
		return err
	}
	for _, recs := range batches {
		if len(recs) == 0 {
			continue
		}
		if err := e.sst.Flush(recs); err != nil {
			return err
		}
	}
	return nil
}

type checkpointFile struct {
	src     string
	relDest string
}

func (e *Engine) listCheckpointFiles() ([]checkpointFile, error) {
	out := make([]checkpointFile, 0)

	// 1) All SSTable files.
	sstRoot := filepath.Join(e.cfg.DataDir, "sstable")
	if _, err := os.Stat(sstRoot); err == nil {
		err = filepath.WalkDir(sstRoot, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !d.Type().IsRegular() {
				return nil
			}
			rel, err := filepath.Rel(sstRoot, path)
			if err != nil {
				return err
			}
			out = append(out, checkpointFile{
				src:     path,
				relDest: filepath.Join("sstable", rel),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	// 2) Optional manifest files in data root.
	manifestCandidates := []string{
		"manifest",
		"MANIFEST",
		"manifest.json",
		"manifest.yaml",
		"manifest.yml",
	}
	for _, name := range manifestCandidates {
		p := filepath.Join(e.cfg.DataDir, name)
		info, err := os.Stat(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		out = append(out, checkpointFile{
			src:     p,
			relDest: name,
		})
	}

	return out, nil
}

func (e *Engine) flushMemtable() error {
	records, ok := e.mem.NextFlushBatch()
	if !ok {
		return nil
	}
	if err := e.sst.Flush(records); err != nil {
		return err
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

	sstOps, err := e.sst.GetMergeOperands(structure, key)
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
	baseline, hasBaseline, err := e.captureBaselineIfNeeded(rec.Key, rec.Seq-1)
	if err != nil {
		return err
	}
	if err := e.applyRecordToMem(rec); err != nil {
		return err
	}
	if hasBaseline {
		e.appendHistory(baseline)
	}
	e.appendHistory(rec)
	return nil
}

func (e *Engine) applyBatchRecords(records []model.Record) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	if len(records) == 0 {
		return nil
	}
	if err := e.mem.CanApplyBatchAtomically(records); err != nil {
		return err
	}
	inBatch := false
	committed := false

	// WAL transactional envelope: BEGIN + records + COMMIT.
	if err := e.wal.AppendBatchBegin(); err != nil {
		return err
	}
	inBatch = true
	if e.testHookAfterBatchBegin != nil {
		if err := e.testHookAfterBatchBegin(); err != nil {
			_ = e.wal.AppendBatchAbort()
			_ = e.wal.Sync()
			return err
		}
	}
	for _, rec := range records {
		if err := e.wal.Append(rec); err != nil {
			_ = e.wal.AppendBatchAbort()
			_ = e.wal.Sync()
			return err
		}
	}
	if err := e.wal.AppendBatchCommit(); err != nil {
		if inBatch && !committed {
			_ = e.wal.AppendBatchAbort()
			_ = e.wal.Sync()
		}
		return err
	}
	committed = true
	// COMMIT must be durable before batch becomes visible in memtables.
	if err := e.wal.Sync(); err != nil {
		return err
	}
	if e.testHookAfterBatchSync != nil {
		if err := e.testHookAfterBatchSync(); err != nil {
			return err
		}
	}

	firstSeqByKey := make(map[string]uint64)
	for _, rec := range records {
		if rec.Key == "" || isInternalKey(rec.Key) {
			continue
		}
		if seq, ok := firstSeqByKey[rec.Key]; !ok || rec.Seq < seq {
			firstSeqByKey[rec.Key] = rec.Seq
		}
	}
	baselines := make([]model.Record, 0, len(firstSeqByKey))
	for key, firstSeq := range firstSeqByKey {
		baseline, ok, err := e.captureBaselineIfNeeded(key, firstSeq-1)
		if err != nil {
			return err
		}
		if ok {
			baselines = append(baselines, baseline)
		}
	}

	if err := e.mem.ApplyBatchAtomically(records); err != nil {
		return err
	}
	for _, rec := range baselines {
		e.appendHistory(rec)
	}
	for _, rec := range records {
		e.appendHistory(rec)
	}
	return nil
}
