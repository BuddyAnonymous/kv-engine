package memtable

import (
	"sort"

	"kv-engine/internal/model"
)

type HashMapMemtable struct {
	maxEntries   int
	entriesNum   int
	data         map[string]model.Record
	mergeOps     map[string][]model.Record
	maxBytes     int64
	currentBytes int64
}

func NewHashMapMemtable(maxEntries int, maxBytes int64) Memtable {
	return &HashMapMemtable{
		maxEntries:   maxEntries,
		maxBytes:     maxBytes,
		data:         make(map[string]model.Record),
		mergeOps:     make(map[string][]model.Record),
		entriesNum:   0,
		currentBytes: 0,
	}
}

func (m *HashMapMemtable) Put(r model.Record) {
	if r.Kind == model.RecordKindMergeOperand {
		m.mergeOps[r.Key] = append(m.mergeOps[r.Key], r)
		m.entriesNum++
		m.currentBytes += estimateRecordSize(&r)
		return
	}
	r.Structure = model.StructureTypeNone
	r.Op = model.MergeOpNone

	if old, exists := m.data[r.Key]; exists {
		m.currentBytes -= estimateRecordSize(&old)
	} else {
		m.entriesNum++
	}
	m.data[r.Key] = r
	m.currentBytes += estimateRecordSize(&r)
}

func (m *HashMapMemtable) Get(key string) model.GetResult {
	rec, ok := m.data[key]
	if !ok {
		return model.GetResult{Found: false}
	}

	return model.GetResult{
		Key:       rec.Key,
		Value:     rec.Value,
		Found:     true,
		Tombstone: rec.Tombstone,
		Seq:       rec.Seq,
		ExpiresAt: rec.ExpiresAt,
		Kind:      rec.Kind,
		Structure: rec.Structure,
		Op:        rec.Op,
	}
}

func (m *HashMapMemtable) GetMergeOperands(structure model.StructureType, key string) []model.Record {
	ops, ok := m.mergeOps[key]
	if !ok || len(ops) == 0 {
		return nil
	}
	out := make([]model.Record, 0, len(ops))
	for _, rec := range ops {
		if rec.Kind != model.RecordKindMergeOperand {
			continue
		}
		if rec.Structure != structure {
			continue
		}
		out = append(out, rec)
	}
	return out
}

func (m *HashMapMemtable) Delete(r model.Record) {
	r.Kind = model.RecordKindKV
	r.Structure = model.StructureTypeNone
	r.Op = model.MergeOpNone
	r.Tombstone = true
	r.Value = nil

	if old, exists := m.data[r.Key]; exists {
		m.currentBytes -= estimateRecordSize(&old)
	} else {
		m.entriesNum++
	}

	m.data[r.Key] = r
	m.currentBytes += estimateRecordSize(&r)
}

func (m *HashMapMemtable) IsFull() bool {
	return m.entriesNum >= m.maxEntries || m.currentBytes >= m.maxBytes
}

func (m *HashMapMemtable) DrainSorted() []model.Record {
	keys := make([]string, 0, len(m.data)+len(m.mergeOps))
	for k := range m.data {
		keys = append(keys, k)
	}
	for k := range m.mergeOps {
		if _, hasKV := m.data[k]; hasKV {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]model.Record, 0, m.entriesNum)
	for _, k := range keys {
		if rec, ok := m.data[k]; ok {
			out = append(out, rec)
		}
		if ops, ok := m.mergeOps[k]; ok {
			out = append(out, ops...)
		}
	}
	sortRecordsForFlush(out)

	m.data = make(map[string]model.Record)
	m.mergeOps = make(map[string][]model.Record)
	m.entriesNum = 0
	m.currentBytes = 0
	return out
}

var _ Memtable = (*HashMapMemtable)(nil)
