package model

type Record struct {
	Key       string
	Value     []byte
	Tombstone bool
	Seq       uint64
}

type GetResult struct {
	Value     []byte
	Found     bool
	Tombstone bool
	Seq       uint64
}
