package memtable

import (
	"kv-engine/internal/model"
)

type btreeNode struct {
	leaf     bool
	keys     []string
	records  []model.Record
	children []*btreeNode
}

type BTreeMemtable struct {
	maxEntries   int
	entriesNum   int
	maxBytes     int64
	currentBytes int64

	t    int // minimum degree (npr 16 -> max keys = 2t-1)
	root *btreeNode
}

func NewBTreeMemtable(maxEntries int, maxBytes int64, t int) Memtable {
	return &BTreeMemtable{
		maxEntries:   maxEntries,
		maxBytes:     maxBytes,
		t:            t,
		root:         &btreeNode{leaf: true},
		entriesNum:   0,
		currentBytes: 0,
	}
}

func (m *BTreeMemtable) Put(r model.Record) {
	// overwrite ako postoji
	if old, ok := m.getRecord(r.Key); ok {
		m.currentBytes -= estimateRecordSize(&old)
		m.setRecord(r.Key, r)
		m.currentBytes += estimateRecordSize(&r)
		return
	}

	// insert novog kljuca
	if len(m.root.keys) == 2*m.t-1 {
		// split root
		oldRoot := m.root
		newRoot := &btreeNode{leaf: false, children: []*btreeNode{oldRoot}}
		m.splitChild(newRoot, 0)
		m.root = newRoot
	}
	m.insertNonFull(m.root, r.Key, r)

	m.entriesNum++
	m.currentBytes += estimateRecordSize(&r)
}

func (m *BTreeMemtable) Get(key string) model.GetResult {
	rec, ok := m.getRecord(key)
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

func (m *BTreeMemtable) Delete(r model.Record) {
	r.Tombstone = true
	r.Value = nil
	m.Put(r)
}

func (m *BTreeMemtable) IsFull() bool {
	return m.entriesNum >= m.maxEntries || m.currentBytes >= m.maxBytes
}

func (m *BTreeMemtable) DrainSorted() []model.Record {
	out := make([]model.Record, 0, m.entriesNum)
	m.inOrder(m.root, &out)

	// reset
	m.root = &btreeNode{leaf: true}
	m.entriesNum = 0
	m.currentBytes = 0
	return out
}

/* ---------------- B-tree internals ---------------- */

func (m *BTreeMemtable) inOrder(n *btreeNode, out *[]model.Record) {
	if n == nil {
		return
	}
	if n.leaf {
		for i := 0; i < len(n.keys); i++ {
			*out = append(*out, n.records[i])
		}
		return
	}
	for i := 0; i < len(n.keys); i++ {
		m.inOrder(n.children[i], out)
		*out = append(*out, n.records[i])
	}
	m.inOrder(n.children[len(n.children)-1], out)
}

func (m *BTreeMemtable) getRecord(key string) (model.Record, bool) {
	n := m.root
	for {
		i := lowerBound(n.keys, key)
		if i < len(n.keys) && n.keys[i] == key {
			return n.records[i], true
		}
		if n.leaf {
			return model.Record{}, false
		}
		n = n.children[i]
	}
}

func (m *BTreeMemtable) setRecord(key string, r model.Record) {
	// pretpostavka: key postoji
	n := m.root
	for {
		i := lowerBound(n.keys, key)
		if i < len(n.keys) && n.keys[i] == key {
			n.records[i] = r
			return
		}
		n = n.children[i]
	}
}

func (m *BTreeMemtable) insertNonFull(x *btreeNode, key string, rec model.Record) {

	if x.leaf {
		// ubaci u sortiran niz keys/records
		pos := lowerBound(x.keys, key)
		x.keys = append(x.keys, "")
		x.records = append(x.records, model.Record{})
		copy(x.keys[pos+1:], x.keys[pos:])
		copy(x.records[pos+1:], x.records[pos:])
		x.keys[pos] = key
		x.records[pos] = rec
		return
	}

	// spusti se u dete
	pos := lowerBound(x.keys, key)
	child := x.children[pos]
	if len(child.keys) == 2*m.t-1 {
		m.splitChild(x, pos)
		// posle split-a, odlucimo u koje dete idemo
		if key > x.keys[pos] {
			pos++
		}
	}
	m.insertNonFull(x.children[pos], key, rec)

}

func (m *BTreeMemtable) splitChild(x *btreeNode, i int) {
	// split x.children[i] (puno dete) na dva deteta i podigni srednji kljuc u x
	t := m.t
	y := x.children[i] // puno dete
	z := &btreeNode{leaf: y.leaf}

	// z dobija zadnjih t-1 kljuceva
	z.keys = append(z.keys, y.keys[t:]...)
	z.records = append(z.records, y.records[t:]...)

	// srednji (index t-1) ide gore u x
	midKey := y.keys[t-1]
	midRec := y.records[t-1]

	// y zadrzava prvih t-1 kljuceva
	y.keys = y.keys[:t-1]
	y.records = y.records[:t-1]

	// ako nije leaf, podeli i decu
	if !y.leaf {
		z.children = append(z.children, y.children[t:]...)
		y.children = y.children[:t]
	}

	// ubaci novi key/record u x na poziciju i
	x.keys = append(x.keys, "")
	x.records = append(x.records, model.Record{})
	copy(x.keys[i+1:], x.keys[i:])
	copy(x.records[i+1:], x.records[i:])
	x.keys[i] = midKey
	x.records[i] = midRec

	// ubaci z kao child odmah posle y
	x.children = append(x.children, nil)
	copy(x.children[i+2:], x.children[i+1:])
	x.children[i+1] = z
}

func lowerBound(keys []string, key string) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if keys[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// compile-time check
var _ Memtable = (*BTreeMemtable)(nil)
