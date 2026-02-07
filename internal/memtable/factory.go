package memtable

func NewByType(t string, maxEntries int, maxBytes int64, btreeDegree int) (Memtable, error) {
	switch t {
	case "", "hashmap":
		return NewHashMapMemtable(maxEntries, maxBytes), nil

	case "skiplist":
		return NewSkipListMemtable(maxEntries, maxBytes), nil

	case "btree":
		return NewBTreeMemtable(maxEntries, maxBytes, btreeDegree), nil

	default:
		return NewHashMapMemtable(maxEntries, maxBytes), nil
	}
}
