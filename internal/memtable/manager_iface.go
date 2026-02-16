package memtable

import "kv-engine/internal/model"

type MemtableManagerIface interface {
	Get(key string) model.GetResult
	GetMergeOperands(structure model.StructureType, key string) []model.Record
	ListLiveKeysInRange(startKey, endKey string) ([]string, error)
	ApplyBatchAtomically(records []model.Record) error
	Put(r model.Record) (flushNeeded bool, err error)
	Delete(r model.Record) (flushNeeded bool, err error)
	NextFlushBatch() ([]model.Record, bool)
}
