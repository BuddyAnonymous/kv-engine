package sstable

import "kv-engine/internal/model"

type ManagerIface interface {
	Flush(records []model.Record) error
	Get(key string) ([]byte, bool, error)
	GetMergeOperands(structure model.StructureType, key string) ([]model.Record, error)
	ValidateMerkle(table string) (model.MerkleValidationResult, error)
	AppendProbMeta(rec model.ProbMetaRecord) error
	GetLatestProbMeta(structure model.StructureType, key string) (model.ProbMetaRecord, bool, error)
	MaxProbMetaSeq() (uint64, error)
}
