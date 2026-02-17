package cache

type EntryKind uint8

const (
	EntryKindKV EntryKind = iota + 1
	EntryKindBloomFilter
	EntryKindCountMinSketch
	EntryKindHyperLogLog
	EntryKindSimHash
)

type Key struct {
	Kind EntryKind
	ID   string
}

type Entry struct {
	Kind      EntryKind
	Data      []byte
	Seq       uint64
	Epoch     uint64
	ExpiresAt uint64
	Tombstone bool
}
