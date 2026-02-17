package block

import "sync"

type BlockCache struct {
	mu  sync.Mutex
	lru *LRUList
}

func NewBlockCache(cacheSize int) *BlockCache {
	return &BlockCache{
		lru: NewLRUList(cacheSize),
	}
}

func (c *BlockCache) Get(key BlockKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	return cloneBlock(val), true
}

func (c *BlockCache) Put(key BlockKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lru.Put(key, cloneBlock(data))
}

func cloneBlock(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
