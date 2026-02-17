package cache

import "container/list"

type lruItem struct {
	key   Key
	value Entry
	size  int
}

type LRU struct {
	ll       *list.List
	items    map[Key]*list.Element
	maxBytes int
	used     int
}

func NewLRU(maxBytes int) *LRU {
	return &LRU{
		ll:       list.New(),
		items:    make(map[Key]*list.Element),
		maxBytes: maxBytes,
	}
}

func (l *LRU) Get(key Key) (Entry, bool) {
	elem, ok := l.items[key]
	if !ok {
		return Entry{}, false
	}

	l.ll.MoveToFront(elem)
	item := elem.Value.(*lruItem)
	return item.value, true
}

func (l *LRU) Put(key Key, value Entry, size int) {
	if elem, ok := l.items[key]; ok {
		item := elem.Value.(*lruItem)

		l.used -= item.size
		item.value = value
		item.size = size
		l.used += size

		l.ll.MoveToFront(elem)
		l.evict()
		return
	}

	item := &lruItem{
		key:   key,
		value: value,
		size:  size,
	}

	elem := l.ll.PushFront(item)
	l.items[key] = elem
	l.used += size

	l.evict()
}

func (l *LRU) Delete(key Key) {
	if elem, ok := l.items[key]; ok {
		l.remove(elem)
	}
}

func (l *LRU) Clear() {
	l.ll.Init()
	l.items = make(map[Key]*list.Element)
	l.used = 0
}

func (l *LRU) evict() {
	for l.used > l.maxBytes {
		back := l.ll.Back()
		if back == nil {
			l.used = 0
			return
		}
		l.remove(back)
	}
}

func (l *LRU) remove(elem *list.Element) {
	item := elem.Value.(*lruItem)
	delete(l.items, item.key)
	l.ll.Remove(elem)
	l.used -= item.size
}
