package engine

import (
	"os"
	"path/filepath"

	"kv-engine/internal/block"
	"kv-engine/internal/config"
	"kv-engine/internal/memtable"
	"kv-engine/internal/model"
	"kv-engine/internal/sstable"
	"kv-engine/internal/wal"
)

type Engine struct {
	cfg config.Config
	bm  *block.Manager
	wal *wal.WAL
	mem memtable.Memtable
	sst *sstable.Manager
	seq uint64
}

func New(cfg config.Config) (*Engine, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	mem, err := memtable.NewByType(
		cfg.MemtableType,
		cfg.MemtableMaxEntries,
		cfg.MemtableMaxBytes,
		cfg.BTreeDegree,
	)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg: cfg,
		bm:  block.New(cfg.BlockSize),
		wal: wal.New(),
		mem: mem,
		sst: sstable.New(filepath.Join(cfg.DataDir, "sstable", "level0"), cfg.MultiFileSSTable),
	}

	// TODO: WAL replay -> memtable
	if err := e.wal.Replay(func(r model.Record) error {
		// e.mem.Put(...) / Delete...
		return nil
	}); err != nil {
		return nil, err
	}

	_ = filepath.Join(cfg.DataDir, "wal")
	return e, nil
}

func (e *Engine) Put(key string, value []byte) error {
	e.seq++
	rec := model.Record{Key: key, Value: value, Tombstone: false, Seq: e.seq}

	// 1) WAL prvo
	if err := e.wal.Append(rec); err != nil {
		return err
	}

	// 2) Memtable
	e.mem.Put(rec)

	// 3) Flush kad je puna
	if e.mem.IsFull() {
		return e.flushMemtable()
	}
	return nil
}

func (e *Engine) Delete(key string) error {
	e.seq++
	rec := model.Record{Key: key, Value: nil, Tombstone: true, Seq: e.seq}

	if err := e.wal.Append(rec); err != nil {
		return err
	}
	e.mem.Delete(rec)
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

	// 2) SSTable
	r = e.sst.Get(key)
	if r.Found {
		if r.Tombstone {
			return nil, false, nil
		}
		return r.Value, true, nil
	}

	return nil, false, nil
}

func (e *Engine) flushMemtable() error {
	// uzmi sortirane zapise + resetuj memtable (postaje prazna)
	records := e.mem.DrainSorted()

	// napravi novi SSTable fajl i upiši
	if err := e.sst.Flush(records); err != nil {
		return err
	}

	// (kasnije) nakon uspešnog flush-a: možeš rotirati/čistiti WAL segmente
	return nil
}
