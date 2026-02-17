package engine

import (
	"encoding/binary"
	"time"

	"kv-engine/internal/cache"
	"kv-engine/internal/model"
	"kv-engine/internal/probabilistic/bloom"
	"kv-engine/internal/probabilistic/cms"
	"kv-engine/internal/probabilistic/hll"
)

func (e *Engine) bumpCacheEpoch() {
	e.cacheEpoch++
}

func (e *Engine) getKVFromCache(key string) ([]byte, bool) {
	if e.cache == nil {
		return nil, false
	}

	ck := cache.Key{Kind: cache.EntryKindKV, ID: key}
	entry, ok := e.cache.Get(ck)
	if !ok {
		return nil, false
	}
	if entry.Epoch != e.cacheEpoch || entry.Tombstone {
		e.cache.Delete(ck)
		return nil, false
	}
	if entry.ExpiresAt > 0 && entry.ExpiresAt <= uint64(time.Now().Unix()) {
		e.cache.Delete(ck)
		return nil, false
	}
	return entry.Data, true
}

func (e *Engine) putKVToCache(key string, value []byte, seq uint64, expiresAt uint64) {
	if e.cache == nil {
		return
	}
	e.cache.Put(cache.Key{Kind: cache.EntryKindKV, ID: key}, cache.Entry{
		Kind:      cache.EntryKindKV,
		Data:      value,
		Seq:       seq,
		Epoch:     e.cacheEpoch,
		ExpiresAt: expiresAt,
		Tombstone: false,
	})
}

func (e *Engine) invalidateKVCache(key string) {
	if e.cache == nil {
		return
	}
	e.cache.Delete(cache.Key{Kind: cache.EntryKindKV, ID: key})
}

func (e *Engine) invalidateStructureCache(structure model.StructureType, key string) {
	if e.cache == nil {
		return
	}
	kind, ok := cacheKindForStructure(structure)
	if !ok {
		return
	}
	e.cache.Delete(cache.Key{Kind: kind, ID: key})
}

func (e *Engine) updateStructureCacheOnMergeAdd(rec model.Record) bool {
	if e.cache == nil {
		return false
	}
	if rec.Kind != model.RecordKindMergeOperand || rec.Op != model.MergeOpAdd {
		return false
	}

	switch rec.Structure {
	case model.StructureTypeBloomFilter:
		bf, ok := e.getBloomFromCache(rec.Key)
		if !ok {
			return false
		}
		bf.Add(rec.Value)
		e.putBloomToCache(rec.Key, bf, rec.Seq)
		return true

	case model.StructureTypeCountMinSketch:
		sketch, ok := e.getCMSFromCache(rec.Key)
		if !ok {
			return false
		}
		sketch.Add(rec.Value)
		e.putCMSToCache(rec.Key, sketch, rec.Seq)
		return true

	case model.StructureTypeHyperLogLog:
		structure, ok := e.getHLLFromCache(rec.Key)
		if !ok {
			return false
		}
		structure.Add(rec.Value)
		e.putHLLToCache(rec.Key, structure, rec.Seq)
		return true

	default:
		return false
	}
}

func (e *Engine) getBloomFromCache(key string) (*bloom.BloomFilter, bool) {
	entry, ok := e.getStructureEntryFromCache(cache.EntryKindBloomFilter, key)
	if !ok {
		return nil, false
	}
	bf, err := bloom.Deserialize(entry.Data)
	if err != nil {
		e.cache.Delete(cache.Key{Kind: cache.EntryKindBloomFilter, ID: key})
		return nil, false
	}
	return bf, true
}

func (e *Engine) putBloomToCache(key string, bf *bloom.BloomFilter, seq uint64) {
	if e.cache == nil || bf == nil {
		return
	}
	data, err := bf.Serialize()
	if err != nil {
		return
	}
	e.cache.Put(cache.Key{Kind: cache.EntryKindBloomFilter, ID: key}, cache.Entry{
		Kind:  cache.EntryKindBloomFilter,
		Data:  data,
		Seq:   seq,
		Epoch: e.cacheEpoch,
	})
}

func (e *Engine) getCMSFromCache(key string) (*cms.CountMinSketch, bool) {
	entry, ok := e.getStructureEntryFromCache(cache.EntryKindCountMinSketch, key)
	if !ok {
		return nil, false
	}
	v, err := cms.Deserialize(entry.Data)
	if err != nil {
		e.cache.Delete(cache.Key{Kind: cache.EntryKindCountMinSketch, ID: key})
		return nil, false
	}
	return v, true
}

func (e *Engine) putCMSToCache(key string, sketch *cms.CountMinSketch, seq uint64) {
	if e.cache == nil || sketch == nil {
		return
	}
	data, err := sketch.Serialize()
	if err != nil {
		return
	}
	e.cache.Put(cache.Key{Kind: cache.EntryKindCountMinSketch, ID: key}, cache.Entry{
		Kind:  cache.EntryKindCountMinSketch,
		Data:  data,
		Seq:   seq,
		Epoch: e.cacheEpoch,
	})
}

func (e *Engine) getHLLFromCache(key string) (*hll.HLL, bool) {
	entry, ok := e.getStructureEntryFromCache(cache.EntryKindHyperLogLog, key)
	if !ok {
		return nil, false
	}
	v, err := hll.Deserialize(entry.Data)
	if err != nil {
		e.cache.Delete(cache.Key{Kind: cache.EntryKindHyperLogLog, ID: key})
		return nil, false
	}
	return v, true
}

func (e *Engine) putHLLToCache(key string, structure *hll.HLL, seq uint64) {
	if e.cache == nil || structure == nil {
		return
	}
	data, err := structure.Serialize()
	if err != nil {
		return
	}
	e.cache.Put(cache.Key{Kind: cache.EntryKindHyperLogLog, ID: key}, cache.Entry{
		Kind:  cache.EntryKindHyperLogLog,
		Data:  data,
		Seq:   seq,
		Epoch: e.cacheEpoch,
	})
}

func (e *Engine) getSimHashFingerprintFromCache(key string) (uint64, bool) {
	entry, ok := e.getStructureEntryFromCache(cache.EntryKindSimHash, key)
	if !ok {
		return 0, false
	}
	if len(entry.Data) != 8 {
		e.cache.Delete(cache.Key{Kind: cache.EntryKindSimHash, ID: key})
		return 0, false
	}
	return binary.BigEndian.Uint64(entry.Data), true
}

func (e *Engine) putSimHashFingerprintToCache(key string, fp uint64, seq uint64) {
	if e.cache == nil {
		return
	}
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, fp)
	e.cache.Put(cache.Key{Kind: cache.EntryKindSimHash, ID: key}, cache.Entry{
		Kind:  cache.EntryKindSimHash,
		Data:  data,
		Seq:   seq,
		Epoch: e.cacheEpoch,
	})
}

func (e *Engine) invalidateSimHashCache(key string) {
	if e.cache == nil {
		return
	}
	e.cache.Delete(cache.Key{Kind: cache.EntryKindSimHash, ID: key})
}

func (e *Engine) getStructureEntryFromCache(kind cache.EntryKind, key string) (cache.Entry, bool) {
	if e.cache == nil {
		return cache.Entry{}, false
	}
	ck := cache.Key{Kind: kind, ID: key}
	entry, ok := e.cache.Get(ck)
	if !ok {
		return cache.Entry{}, false
	}
	if entry.Epoch != e.cacheEpoch {
		e.cache.Delete(ck)
		return cache.Entry{}, false
	}
	return entry, true
}

func cacheKindForStructure(structure model.StructureType) (cache.EntryKind, bool) {
	switch structure {
	case model.StructureTypeBloomFilter:
		return cache.EntryKindBloomFilter, true
	case model.StructureTypeCountMinSketch:
		return cache.EntryKindCountMinSketch, true
	case model.StructureTypeHyperLogLog:
		return cache.EntryKindHyperLogLog, true
	default:
		return 0, false
	}
}
