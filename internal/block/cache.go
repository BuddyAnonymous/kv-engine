package block

type BlockCache struct {
	lru *LRUList
}

func NewBlockCache(cacheSize int) *BlockCache {
	return &BlockCache{
		lru: NewLRUList(cacheSize),
	}
}

func (c *BlockCache) Get(key BlockKey) ([]byte, bool) {
	val, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	return val, true
}

func (c *BlockCache) Put(key BlockKey, data []byte) {
	c.lru.Put(key, data)
}
