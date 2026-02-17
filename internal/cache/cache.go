package cache

import "sync"

type Cache struct {
	mu  sync.Mutex
	lru *LRU
}

func New(maxBytes int) *Cache {
	if maxBytes < 0 {
		maxBytes = 0
	}

	return &Cache{
		lru: NewLRU(maxBytes),
	}
}

func (c *Cache) Get(key Key) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.lru.Get(key)
	if !ok {
		return Entry{}, false
	}

	return cloneEntry(entry), true
}

func (c *Cache) Put(key Key, entry Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry.Kind = key.Kind
	entry = cloneEntry(entry)

	size := estimateItemSize(key, entry)

	// ako je entry veći od celog cache-a brise ukoliko je postojala stara verzija
	if size > c.lru.maxBytes {
		c.lru.Delete(key)
		return
	}

	c.lru.Put(key, entry, size)
}

func (c *Cache) Delete(key Key) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lru.Delete(key)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lru.Clear()
}

func estimateItemSize(key Key, entry Entry) int {
	return len(key.ID) + len(entry.Data)
}

func cloneEntry(e Entry) Entry {
	out := e
	out.Data = cloneBytes(e.Data)
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
