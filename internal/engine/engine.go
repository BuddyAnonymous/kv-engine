package engine

import (
	"os"
	"path/filepath"
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
	rec := model.Record{Key: key, Value: value, Tombstone: false, Seq: e.seq, ExpiresAt: expiresAt}

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

func (e *Engine) Delete(key string) error {
	e.seq++
	rec := model.Record{Key: key, Value: nil, Tombstone: true, Seq: e.seq, ExpiresAt: 0}

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
	// 1) Memtable
	r := e.mem.Get(key)
	if r.Found {
		if r.Tombstone {
			return nil, false, nil
		}
		return r.Value, true, nil
	}

	return nil, false, nil
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
