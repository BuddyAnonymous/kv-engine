package memtable

import (
	"fmt"
	"kv-engine/internal/config"
)

func FactoryFromConfig(cfg config.Config) (Factory, error) {
	switch cfg.MemtableType {
	case "", "hashmap":
		return func() Memtable {
			return NewHashMapMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxBytes)
		}, nil
	case "skiplist":
		return func() Memtable {
			return NewSkipListMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxBytes)
		}, nil
	case "btree":
		return func() Memtable {
			return NewBTreeMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxBytes, cfg.BTreeDegree)
		}, nil
	default:
		return nil, fmt.Errorf("unknown memtable_type: %q", cfg.MemtableType)
	}
}
