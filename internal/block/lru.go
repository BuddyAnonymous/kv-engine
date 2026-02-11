package block

import (
	"container/list"
)

// LRUList je generička pomoćna lista za LRU cache
type LRUList struct {
	ll          *list.List
	table       map[BlockKey]*list.Element
	cacheSize   int
	currentSize int
}

// entry u LRU listi
type lruEntry struct {
	key   BlockKey
	value []byte
}

func NewLRUList(size int) *LRUList {
	return &LRUList{
		ll:          list.New(),
		table:       make(map[BlockKey]*list.Element),
		cacheSize:   size,
		currentSize: 0,
	}
}

// Get vraća vrednost i pomera entry na vrh (most recently used)
func (l *LRUList) Get(key BlockKey) ([]byte, bool) {
	if elem, ok := l.table[key]; ok {
		l.ll.MoveToFront(elem)
		return elem.Value.(*lruEntry).value, true
	}
	return nil, false
}

// Put dodaje ili osvežava vrednost i premesta je na vrh
func (l *LRUList) Put(key BlockKey, value []byte) {
	if elem, ok := l.table[key]; ok {
		elem.Value.(*lruEntry).value = value
		l.ll.MoveToFront(elem)
		return
	}

	entry := &lruEntry{key, value}
	elem := l.ll.PushFront(entry)
	l.table[key] = elem
	l.currentSize += len(value)

	// Ako je prekoračena veličina cache-a, ukloni najstariji entry
	for l.currentSize > l.cacheSize {
		l.removeOldest()
	}
}

func (l *LRUList) removeOldest() {
	elem := l.ll.Back()
	if elem != nil {
		l.ll.Remove(elem)
		entry := elem.Value.(*lruEntry)
		delete(l.table, entry.key)
		l.currentSize -= len(entry.value)
	}
}
