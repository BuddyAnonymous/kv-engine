package model

type Record struct {
	Key       string
	Value     []byte
	Tombstone bool
	Seq       uint64
	ExpiresAt uint64
}

type GetResult struct {
	Key       string
	Value     []byte
	Found     bool
	Tombstone bool
	Seq       uint64
	ExpiresAt uint64
}

type IndexEntry struct {
	Key        string
	DataOffset uint64
}

type SummaryEntry struct {
	Key         string
	IndexOffset uint64
}

type BloomFilter struct {
	MBits uint64
	K     uint32
	Seed  uint64
	Bits  []byte
}

type MerkleTree struct {
	Root       [32]byte
	LeafHashes [][32]byte
}

type SSTFooter struct {
	IndexOffset   uint64
	IndexLen      uint64
	SummaryOffset uint64
	SummaryLen    uint64
	FilterOffset  uint64
	FilterLen     uint64
	MerkleOffset  uint64
	MerkleLen     uint64
}
