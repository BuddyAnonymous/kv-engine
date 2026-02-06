package memtable

import (
	"sort"

	"kv-engine/internal/model"
)

type Memtable struct {
	maxEntries int
	data       map[string]model.Record // čuvamo poslednji record po key
}

func New(maxEntries int) *Memtable {
	return &Memtable{
		maxEntries: maxEntries,
		data:       make(map[string]model.Record),
	}
}

func (m *Memtable) Put(r model.Record) {
	m.data[r.Key] = r
}

func (m *Memtable) Get(key string) model.GetResult {
	rec, ok := m.data[key]
	if !ok {
		return model.GetResult{Found: false}
	}

	return model.GetResult{
		Value:     rec.Value,
		Found:     true,
		Tombstone: rec.Tombstone,
		Seq:       rec.Seq,
	}
}

func (m *Memtable) Delete(r model.Record) {
	r.Tombstone = true
	r.Value = nil
	m.data[r.Key] = r
}

func (m *Memtable) IsFull() bool {
	return len(m.data) >= m.maxEntries
}

// DrainSorted: vrati sve zapise sortirane po ključu i isprazni memtable
func (m *Memtable) DrainSorted() []model.Record {
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
	return out
}
