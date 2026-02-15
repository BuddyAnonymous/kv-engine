package bloom

import "kv-engine/internal/model"

// Rekonstruiše Bloom iz merge operacija
func BuildBloomFromOps(ops []model.Record) *BloomFilter {
	// U ostatku sistemu gleda se iz meta bloka SSTable
	// Za BF operacije je za sada default
	bf := NewBloomFilter(100000, 0.01)

	for _, rec := range ops {
		if rec.Op == model.MergeOpAdd {
			bf.Add(rec.Value)
		}
	}
	return bf
}
