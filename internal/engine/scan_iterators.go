package engine

import (
	"fmt"
	"sort"
	"time"

	"kv-engine/internal/model"
)

type scanIterator struct {
	results []model.KVPair
	pos     int
}

func (e *Engine) PrefixScan(prefix string, pageNumber, pageSize int) ([]model.KVPair, error) {
	if pageNumber < 1 {
		return nil, fmt.Errorf("pageNumber must be >= 1")
	}
	if pageSize < 1 {
		return nil, fmt.Errorf("pageSize must be >= 1")
	}

	all, err := e.collectVisibleSortedPairs(func(key string) bool {
		return len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix)
	})
	if err != nil {
		return nil, err
	}
	return paginatePairs(all, pageNumber, pageSize), nil
}

func (e *Engine) RangeScan(minKey, maxKey string, pageNumber, pageSize int) ([]model.KVPair, error) {
	if pageNumber < 1 {
		return nil, fmt.Errorf("pageNumber must be >= 1")
	}
	if pageSize < 1 {
		return nil, fmt.Errorf("pageSize must be >= 1")
	}
	if minKey > maxKey {
		return nil, fmt.Errorf("invalid range: minKey > maxKey")
	}

	all, err := e.collectVisibleSortedPairs(func(key string) bool {
		return key >= minKey && key <= maxKey
	})
	if err != nil {
		return nil, err
	}
	return paginatePairs(all, pageNumber, pageSize), nil
}

func (e *Engine) PrefixIterate(prefix string) (uint64, error) {
	all, err := e.collectVisibleSortedPairs(func(key string) bool {
		return len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix)
	})
	if err != nil {
		return 0, err
	}
	return e.newIterator(all), nil
}

func (e *Engine) RangeIterate(minKey, maxKey string) (uint64, error) {
	if minKey > maxKey {
		return 0, fmt.Errorf("invalid range: minKey > maxKey")
	}
	all, err := e.collectVisibleSortedPairs(func(key string) bool {
		return key >= minKey && key <= maxKey
	})
	if err != nil {
		return 0, err
	}
	return e.newIterator(all), nil
}

func (e *Engine) IteratorNext(id uint64) (model.KVPair, bool, error) {
	it, ok := e.iterators[id]
	if !ok {
		return model.KVPair{}, false, fmt.Errorf("iterator not found")
	}
	if it.pos >= len(it.results) {
		return model.KVPair{}, false, nil
	}
	pair := it.results[it.pos]
	it.pos++
	return clonePair(pair), true, nil
}

func (e *Engine) IteratorStop(id uint64) error {
	if _, ok := e.iterators[id]; !ok {
		return fmt.Errorf("iterator not found")
	}
	delete(e.iterators, id)
	return nil
}

func (e *Engine) newIterator(results []model.KVPair) uint64 {
	e.nextIteratorID++
	id := e.nextIteratorID
	e.iterators[id] = &scanIterator{results: clonePairs(results)}
	return id
}

func (e *Engine) collectVisibleSortedPairs(filter func(string) bool) ([]model.KVPair, error) {
	memRecs := e.mem.SnapshotSorted()
	sstRecs, err := e.lsm.CollectAllRecords()
	if err != nil {
		return nil, err
	}

	all := make([]model.Record, 0, len(memRecs)+len(sstRecs))
	all = append(all, memRecs...)
	all = append(all, sstRecs...)

	latestByKey := make(map[string]model.Record)
	for _, rec := range all {
		if rec.Kind != model.RecordKindKV {
			continue
		}
		if isInternalSystemKey(rec.Key) {
			continue
		}
		if filter != nil && !filter(rec.Key) {
			continue
		}
		old, ok := latestByKey[rec.Key]
		if !ok || rec.Seq > old.Seq {
			latestByKey[rec.Key] = rec
		}
	}

	keys := make([]string, 0, len(latestByKey))
	for k := range latestByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	now := uint64(time.Now().Unix())
	out := make([]model.KVPair, 0, len(keys))
	for _, k := range keys {
		rec := latestByKey[k]
		if rec.Tombstone {
			continue
		}
		if rec.ExpiresAt > 0 && rec.ExpiresAt <= now {
			continue
		}
		out = append(out, model.KVPair{
			Key:   rec.Key,
			Value: clonePairValue(rec.Value),
		})
	}
	return out, nil
}

func paginatePairs(in []model.KVPair, pageNumber, pageSize int) []model.KVPair {
	start := (pageNumber - 1) * pageSize
	if start >= len(in) {
		return []model.KVPair{}
	}

	end := start + pageSize
	if end > len(in) {
		end = len(in)
	}
	return clonePairs(in[start:end])
}

func clonePairs(in []model.KVPair) []model.KVPair {
	out := make([]model.KVPair, len(in))
	for i := range in {
		out[i] = clonePair(in[i])
	}
	return out
}

func clonePair(in model.KVPair) model.KVPair {
	return model.KVPair{
		Key:   in.Key,
		Value: clonePairValue(in.Value),
	}
}

func clonePairValue(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
