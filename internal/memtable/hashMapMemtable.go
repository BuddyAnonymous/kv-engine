package memtable

import (
	"sort"

	"kv-engine/internal/model"
)

type HashMapMemtable struct {
	maxEntries   int
	entriesNum   int
	data         map[string]model.Record // čuvamo poslednji record po key
	maxBytes     int64
	currentBytes int64
}

func NewHashMapMemtable(maxEntries int, maxBytes int64) Memtable {
	return &HashMapMemtable{
		maxEntries:   maxEntries,
		maxBytes:     maxBytes,
		data:         make(map[string]model.Record),
		entriesNum:   0,
		currentBytes: 0,
	}
}

func (m *HashMapMemtable) Put(r model.Record) {
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
	}
}

func (m *HashMapMemtable) Delete(r model.Record) {
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

// DrainSorted: vrati sve zapise sortirane po ključu i isprazni memtable
func (m *HashMapMemtable) DrainSorted() []model.Record {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]model.Record, 0, len(keys))
	for _, k := range keys {
		out = append(out, m.data[k])
	}

	// reset
	m.data = make(map[string]model.Record)
	m.entriesNum = 0
	m.currentBytes = 0
	return out
}

// compile-time check (opciono, ali korisno)
var _ Memtable = (*HashMapMemtable)(nil)
