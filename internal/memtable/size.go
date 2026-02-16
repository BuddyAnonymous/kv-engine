package memtable

import "kv-engine/internal/model"

func estimateRecordSize(r *model.Record) int64 {
	size := 0

	size += len(r.Key)
	size += len(r.Value)

	size += 1 // Tombstone
	size += 8 // Seq
	size += 8 // ExpiresAt
	size += 1 // Kind
	size += 1 // Structure
	size += 1 // Op

	size += 32 // overhead (string header + slice header + mapa)

	return int64(size)
}
