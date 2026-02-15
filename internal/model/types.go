package model

type RecordKind uint8

const (
	RecordKindKV RecordKind = iota
	RecordKindMergeOperand
)

type StructureType uint8

const (
	StructureTypeNone StructureType = iota
	StructureTypeBloomFilter
	StructureTypeCountMinSketch
	StructureTypeHyperLogLog
)

type MergeOpType uint8

const (
	MergeOpNone MergeOpType = iota
	MergeOpAdd
	MergeOpRemove
)

type Record struct {
	Key       string
	Value     []byte
	Tombstone bool
	Seq       uint64
	ExpiresAt uint64
	Kind      RecordKind
	Structure StructureType
	Op        MergeOpType
}

type GetResult struct {
	Key       string
	Value     []byte
	Found     bool
	Tombstone bool
	Seq       uint64
	ExpiresAt uint64
	Kind      RecordKind
	Structure StructureType
	Op        MergeOpType
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
