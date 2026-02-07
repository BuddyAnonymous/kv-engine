package memtable

import "kv-engine/internal/model"

func estimateRecordSize(r *model.Record) int64 {
	size := 0

	size += len(r.Key)
	size += len(r.Value)

	size += 1 // Tombstone
	size += 8 // Seq

	size += 32 // overhead (string header + slice header + mapa)

	return int64(size)
}
