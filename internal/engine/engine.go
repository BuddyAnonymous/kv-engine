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
	"kv-engine/internal/memtable"
	"kv-engine/internal/model"
	"kv-engine/internal/sstable"
	"kv-engine/internal/wal"
)

type Engine struct {
	cfg config.Config
	bm  *block.BlockManager
	wal *wal.WAL
	mem memtable.MemtableManagerIface
	sst *sstable.Manager
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

	e := &Engine{
		cfg: cfg,
		bm:  bm,
		wal: wal.New(),
		mem: mem,
		sst: sstable.New(filepath.Join(cfg.DataDir, "sstable", "level0"), cfg.MultiFileSSTable, bm, cfg.BlockSize, uint64(cfg.SummaryStride)),
	}

	// TODO: WAL replay -> memtable
	if err := e.wal.Replay(func(r model.Record) error {
		// e.mem.Put(...) / Delete...
		return nil
	}); err != nil {
		return nil, err
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

	// 1) WAL prvo
	if err := e.wal.Append(rec); err != nil {
		return err
	}

	// 2) Memtable
	flushNeeded, err := e.mem.Put(rec)
	if err != nil {
		return err
	}

	// 3) Flush kad je puna
	if flushNeeded {

		return e.flushMemtable()
	}
	return nil
}

func (e *Engine) Merge(structure model.StructureType, key string, value []byte, op model.MergeOpType, ttl ...time.Duration) error {
	if structure == model.StructureTypeNone {
		return fmt.Errorf("invalid merge structure type")
	}
	if op != model.MergeOpAdd && op != model.MergeOpRemove {
		return fmt.Errorf("invalid merge op type")
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

	if err := e.wal.Append(rec); err != nil {
		return err
	}

	flushNeeded, err := e.mem.Put(rec)
	if err != nil {
		return err
	}
	if flushNeeded {
		return e.flushMemtable()
	}
	return nil
}

func (e *Engine) BFAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeBloomFilter, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) BFRemove(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeBloomFilter, key, value, model.MergeOpRemove, ttl...)
}

func (e *Engine) CMSAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeCountMinSketch, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) CMSRemove(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeCountMinSketch, key, value, model.MergeOpRemove, ttl...)
}

func (e *Engine) HLLAdd(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeHyperLogLog, key, value, model.MergeOpAdd, ttl...)
}

func (e *Engine) HLLRemove(key string, value []byte, ttl ...time.Duration) error {
	return e.Merge(model.StructureTypeHyperLogLog, key, value, model.MergeOpRemove, ttl...)
}

func (e *Engine) BFGet(key string, value []byte) (bool, error) {
	ops, err := e.getAllMergeOperands(model.StructureTypeBloomFilter, key)
	if err != nil {
		return false, err
	}

	present := make(map[string]struct{}, len(ops))
	for _, rec := range ops {
		v := string(rec.Value)
		switch rec.Op {
		case model.MergeOpAdd:
			present[v] = struct{}{}
		case model.MergeOpRemove:
			delete(present, v)
		}
	}

	_, ok := present[string(value)]
	return ok, nil
}

func (e *Engine) CMSGet(key string, value []byte) (uint64, error) {
	ops, err := e.getAllMergeOperands(model.StructureTypeCountMinSketch, key)
	if err != nil {
		return 0, err
	}

	var count uint64
	for _, rec := range ops {
		if !bytes.Equal(rec.Value, value) {
			continue
		}
		switch rec.Op {
		case model.MergeOpAdd:
			count++
		case model.MergeOpRemove:
			if count > 0 {
				count--
			}
		}
	}
	return count, nil
}

func (e *Engine) HLLGet(key string) (uint64, error) {
	ops, err := e.getAllMergeOperands(model.StructureTypeHyperLogLog, key)
	if err != nil {
		return 0, err
	}

	set := make(map[string]struct{}, len(ops))
	for _, rec := range ops {
		v := string(rec.Value)
		switch rec.Op {
		case model.MergeOpAdd:
			set[v] = struct{}{}
		case model.MergeOpRemove:
			delete(set, v)
		}
	}
	return uint64(len(set)), nil
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

	if err := e.wal.Append(rec); err != nil {
		return err
	}
	flushNeeded, err := e.mem.Delete(rec)
	if err != nil {
		return err
	}
	if flushNeeded {
		return e.flushMemtable()
	}
	return nil
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

	// 2) SSTable (L0 newest -> oldest)
	val, found, err := e.sst.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return val, true, nil
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
		if rec.Op != model.MergeOpAdd && rec.Op != model.MergeOpRemove {
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
