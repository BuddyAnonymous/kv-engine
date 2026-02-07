package memtable

import "kv-engine/internal/model"

type Memtable interface {
	Put(r model.Record)
	Delete(r model.Record)
	Get(key string) model.GetResult

	// za flush:
	DrainSorted() []model.Record

	// za kontrolu punjenja:
	IsFull() bool
}
