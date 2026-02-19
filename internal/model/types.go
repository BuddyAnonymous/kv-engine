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
)

type ProbMetaAction string

const (
	ProbMetaActionCreate ProbMetaAction = "create"
	ProbMetaActionDelete ProbMetaAction = "delete"
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

type KVPair struct {
	Key   string
	Value []byte
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

type MerkleValidationResult struct {
	Valid              bool
	ChangedLeafIndices []int
	ExpectedRootHex    string
	ActualRootHex      string
	ExpectedLeafCount  int
	ActualLeafCount    int
}

type ProbMetaRecord struct {
	Structure StructureType  `json:"structure"`
	Key       string         `json:"key"`
	Action    ProbMetaAction `json:"action"`
	Seq       uint64         `json:"seq"`

	// Bloom Filter params
	BFExpectedElements  uint64  `json:"bf_expected_elements,omitempty"`
	BFFalsePositiveRate float64 `json:"bf_false_positive_rate,omitempty"`
	BFSeed              uint32  `json:"bf_seed,omitempty"`

	// Count-Min Sketch params
	CMSEpsilon float64 `json:"cms_epsilon,omitempty"`
	CMSDelta   float64 `json:"cms_delta,omitempty"`
	CMSSeed    uint32  `json:"cms_seed,omitempty"`

	// HyperLogLog params
	HLLPrecision uint8  `json:"hll_precision,omitempty"`
	HLLSeed      uint32 `json:"hll_seed,omitempty"`
}
