package engine

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"kv-engine/internal/backup"
	"kv-engine/internal/block"
	"kv-engine/internal/cache"
	"kv-engine/internal/config"
	"kv-engine/internal/lsm"
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

	iterators      map[uint64]*scanIterator
	nextIteratorID uint64

	rl      *ratelimit.TokenBucket
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
		iterators:  make(map[uint64]*scanIterator),
		history:    make(map[string][]model.Record),
		snapshots:  make(map[string]snapshotPoint),
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
	return strings.HasPrefix(key, internalKeyPrefix) || isInternalSystemKey(key)
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
	if err := e.sst.AppendProbMeta(meta); err != nil {
		return err
	}
	e.invalidateStructureCache(model.StructureTypeHyperLogLog, key)
	return nil
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
		e.invalidateStructureCache(model.StructureTypeBloomFilter, key)
		return false, nil
	}
	if bf, ok := e.getBloomFromCache(key); ok {
		return bf.MightContain(value), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeBloomFilter, key, meta.Seq)
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
		e.invalidateStructureCache(model.StructureTypeCountMinSketch, key)
		return 0, nil
	}
	if sketch, ok := e.getCMSFromCache(key); ok {
		return sketch.Estimate(value), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeCountMinSketch, key, meta.Seq)
	if err != nil {
		return 0, err
	}

	sketch := cms.Merge(ops, meta.CMSEpsilon, meta.CMSDelta)
	e.putCMSToCache(key, sketch, maxSeq(meta.Seq, maxSeqFromRecords(ops)))
	return sketch.Estimate(value), nil
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
		e.invalidateStructureCache(model.StructureTypeHyperLogLog, key)
		return 0, nil
	}
	if structure, ok := e.getHLLFromCache(key); ok {
		return uint64(structure.Estimate()), nil
	}

	ops, err := e.getAllMergeOperands(model.StructureTypeHyperLogLog, key, meta.Seq)
	if err != nil {
		return 0, err
	}

	structure := hll.Merge(ops, meta.HLLPrecision, meta.HLLSeed)
	e.putHLLToCache(key, structure, maxSeq(meta.Seq, maxSeqFromRecords(ops)))
	return uint64(structure.Estimate()), nil
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
	if val, ok := e.getKVFromCache(key); ok {
		return val, true, nil
	}

	now := uint64(time.Now().Unix())

	r := e.mem.Get(key)
	if r.Found {
		if r.Tombstone || (r.ExpiresAt > 0 && r.ExpiresAt <= now) {
			e.invalidateKVCache(key)
			return nil, false, nil
		}
		e.putKVToCache(key, r.Value, r.Seq, r.ExpiresAt)
		return r.Value, true, nil
	}

	rec, found, err := e.lsm.GetRecord(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		e.invalidateKVCache(key)
		return nil, false, nil
	}
	e.putKVToCache(key, rec.Value, rec.Seq, rec.ExpiresAt)
	return rec.Value, true, nil
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

func (e *Engine) getAllMergeOperands(structure model.StructureType, key string, createSeq uint64) ([]model.Record, error) {
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
		// Skip operands from a previous create epoch.
		if createSeq > 0 && rec.Seq < createSeq {
			continue
		}
		ops = append(ops, rec)
	}

	sstOps, err := e.lsm.GetMergeOperands(structure, key)
	if err != nil {
		return nil, err
	}
	// Filter SSTable operands by epoch boundary too.
	for _, rec := range sstOps {
		if createSeq > 0 && rec.Seq < createSeq {
			continue
		}
		ops = append(ops, rec)
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

// reloadRuntimeAfterRestore potpuno osvezava runtime state engine-a nakon restore-a sa diska.
func (e *Engine) reloadRuntimeAfterRestore() error {
	fact, err := memtable.FactoryFromConfig(e.cfg)
	if err != nil {
		return err
	}
	mem, err := memtable.NewMemtableManager(e.cfg.MemtableInstances, fact)
	if err != nil {
		return err
	}

	bm := block.NewBlockManager(e.cfg.BlockCacheSize)
	sstBaseDir := filepath.Join(e.cfg.DataDir, "sstable")
	sstMgr := sstable.New(filepath.Join(sstBaseDir, "level0"), e.cfg.MultiFileSSTable, bm, e.cfg.BlockSize, uint64(e.cfg.SummaryStride))
	lsmCfg := lsm.LSMConfig{
		MaxLevels:             e.cfg.LSMMaxLevels,
		Algorithm:             e.cfg.LSMCompactionAlgorithm,
		SizeTieredMinSSTables: e.cfg.LSMSizeTieredMinSSTables,
		LeveledL0Threshold:    e.cfg.LSMLeveledL0Threshold,
		LeveledBaseSizeMB:     e.cfg.LSMLeveledBaseSizeMB,
		LeveledMultiplier:     e.cfg.LSMLeveledMultiplier,
	}
	lsmTree, err := lsm.NewLSMTree(lsmCfg, sstMgr, sstBaseDir)
	if err != nil {
		return fmt.Errorf("failed to create lsm tree: %w", err)
	}
	backupMgr, err := backup.NewBackupManager(bm, e.cfg.BlockSize)
	if err != nil {
		return fmt.Errorf("failed to create backup manager: %w", err)
	}

	// Rekreiramo sve runtime komponente koje drze state u memoriji.
	e.seq = 0
	e.bm = bm
	e.cache = cache.New(e.cfg.CacheSize)
	e.cacheEpoch = 1
	e.mem = mem
	e.sst = sstMgr
	e.lsm = lsmTree
	e.backupMgr = backupMgr
	e.iterators = make(map[uint64]*scanIterator)
	e.nextIteratorID = 0
	e.wal = nil

	walManager, walErr, lastSeq := wal.NewWALManager(filepath.Join(e.cfg.DataDir, "wal"), e.cfg.SegmentBlocks, e.cfg.BlockSize, bm, e)
	if walErr != nil {
		return walErr
	}
	e.wal = walManager
	if lastSeq > e.seq {
		e.seq = lastSeq
	}

	maxProbMetaSeq, err := e.sst.MaxProbMetaSeq()
	if err != nil {
		return err
	}
	if maxProbMetaSeq > e.seq {
		e.seq = maxProbMetaSeq
	}

	return nil
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
		if isInternalKey(rec.Key) {
			continue
		}
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
			if err := e.reloadRuntimeAfterRestore(); err != nil {
				fmt.Println("restore uradjen, ali osvezavanje engine stanja nije uspelo:", err)
				continue
			}
			fmt.Println("Restore zavrsen uspesno.")

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
