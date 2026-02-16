package memtable

import (
	"sort"

	"kv-engine/internal/model"
)

type Memtable interface {
	Put(r model.Record)
	Delete(r model.Record)
	Get(key string) model.GetResult
	GetMergeOperands(structure model.StructureType, key string) []model.Record

	// za flush:
	DrainSorted() []model.Record

	// za kontrolu punjenja:
	IsFull() bool
}

func sortRecordsForFlush(records []model.Record) {
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Key != records[j].Key {
			return records[i].Key < records[j].Key
		}
		if records[i].Structure != records[j].Structure {
			return records[i].Structure < records[j].Structure
		}
		if records[i].Op != records[j].Op {
			return records[i].Op < records[j].Op
		}
		if records[i].Seq != records[j].Seq {
			return records[i].Seq < records[j].Seq
		}
		return records[i].Kind < records[j].Kind
	})
}
