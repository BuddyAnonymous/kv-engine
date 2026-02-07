package memtable

import (
	"math/rand"
	"time"

	"kv-engine/internal/model"
)

const (
	defaultMaxLevel = 16
	defaultP        = 0.5
)

type skipNode struct {
	key     string
	rec     model.Record
	forward []*skipNode
}

type SkipListMemtable struct {
	maxEntries   int
	entriesNum   int
	maxBytes     int64
	currentBytes int64

	head     *skipNode
	level    int
	maxLevel int
	p        float64

	rng *rand.Rand
}

func NewSkipListMemtable(maxEntries int, maxBytes int64) Memtable {
	head := &skipNode{
		forward: make([]*skipNode, defaultMaxLevel),
	}
	return &SkipListMemtable{
		maxEntries:   maxEntries,
		maxBytes:     maxBytes,
		head:         head,
		level:        1,
		maxLevel:     defaultMaxLevel,
		p:            defaultP,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		entriesNum:   0,
		currentBytes: 0,
	}
}

func (m *SkipListMemtable) Put(r model.Record) {
	// update[] za svaku visinu
	update := make([]*skipNode, m.maxLevel)
	x := m.head

	for i := m.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < r.Key {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]
	if x != nil && x.key == r.Key {
		// overwrite postojeceg kljuca
		m.currentBytes -= estimateRecordSize(&x.rec)
		x.rec = r
		m.currentBytes += estimateRecordSize(&x.rec)
		return
	}

	// insert novog kljuca
	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	newNode := &skipNode{
		key:     r.Key,
		rec:     r,
		forward: make([]*skipNode, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	m.entriesNum++
	m.currentBytes += estimateRecordSize(&newNode.rec)
}

func (m *SkipListMemtable) Get(key string) model.GetResult {
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x == nil || x.key != key {
		return model.GetResult{Found: false}
	}
	return model.GetResult{
		Key:       x.key,
		Value:     x.rec.Value,
		Found:     true,
		Tombstone: x.rec.Tombstone,
		Seq:       x.rec.Seq,
	}
}

func (m *SkipListMemtable) Delete(r model.Record) {
	r.Tombstone = true
	r.Value = nil
	m.Put(r) // isti key overwrite/insert, samo sa tombstone
}

func (m *SkipListMemtable) IsFull() bool {
	return m.entriesNum >= m.maxEntries || m.currentBytes >= m.maxBytes
}

func (m *SkipListMemtable) DrainSorted() []model.Record {
	out := make([]model.Record, 0, m.entriesNum)

	for x := m.head.forward[0]; x != nil; x = x.forward[0] {
		out = append(out, x.rec)
	}

	// reset
	m.head = &skipNode{forward: make([]*skipNode, m.maxLevel)}
	m.level = 1
	m.entriesNum = 0
	m.currentBytes = 0

	return out
}

func (m *SkipListMemtable) randomLevel() int {
	lvl := 1
	for lvl < m.maxLevel && m.rng.Float64() < m.p {
		lvl++
	}
	return lvl
}

// compile-time check (opciono, ali korisno)
var _ Memtable = (*SkipListMemtable)(nil)
