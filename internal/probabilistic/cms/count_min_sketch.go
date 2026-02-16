package cms

import (
	"encoding/binary"
	"errors"
	"kv-engine/internal/model"
)

var (
	ErrInvalidCMSData = errors.New("invalid Count-Min Sketch data")
	ErrInvalidCMSMeta = errors.New("invalid Count-Min Sketch meta")
)

type CountMinSketch struct {
	m      uint
	k      uint
	seed   uint32
	table  [][]uint64
	hashes []HashWithSeed
}

// Konstruktor
func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)

	table := make([][]uint64, k)
	for i := uint(0); i < k; i++ {
		table[i] = make([]uint64, m)
	}

	hashes, seed := CreateHashFunctions(k)

	return &CountMinSketch{
		m:      m,
		k:      k,
		seed:   seed,
		table:  table,
		hashes: hashes,
	}
}

// Kreiranje iz meta bloka
func NewFromMeta(m uint, k uint, seed uint32) *CountMinSketch {
	hashes := CreateHashFunctionsWithSeed(uint32(k), seed)

	return &CountMinSketch{
		m:      m,
		k:      k,
		seed:   seed,
		table:  make([][]uint64, k),
		hashes: hashes,
	}
}

func (cms *CountMinSketch) Add(data []byte) {
	for i := uint(0); i < cms.k; i++ {
		hash := cms.hashes[i].Hash(data)
		index := hash % uint64(cms.m)
		cms.table[i][index]++
	}
}

func (cms *CountMinSketch) Estimate(data []byte) uint64 {
	var min uint64 = ^uint64(0)

	for i := uint(0); i < cms.k; i++ {
		hash := cms.hashes[i].Hash(data)
		index := hash % uint64(cms.m)
		val := cms.table[i][index]

		if val < min {
			min = val
		}
	}
	return min
}

func (cms *CountMinSketch) Merge(other *CountMinSketch) error {
	if cms.m != other.m || cms.k != other.k || cms.seed != other.seed {
		return ErrInvalidCMSMeta
	}

	for i := uint(0); i < cms.k; i++ {
		for j := uint(0); j < cms.m; j++ {
			cms.table[i][j] += other.table[i][j]
		}
	}
	return nil
}

// Format:
// [0:4] -> m
// [4:8] -> k
// [8:12] -> seed
// [12:]  -> table (k*m uint64 vrednosti)
func (cms *CountMinSketch) Serialize() ([]byte, error) {
	buf := make([]byte, 12+len(cms.table)*len(cms.table[0])*8)

	binary.BigEndian.PutUint32(buf[0:4], uint32(cms.m))
	binary.BigEndian.PutUint32(buf[4:8], uint32(cms.k))
	binary.BigEndian.PutUint32(buf[8:12], cms.seed)

	for i := uint(0); i < cms.k; i++ {
		for j := uint(0); j < cms.m; j++ {
			offset := 12 + int(i*cms.m+j)*8
			binary.BigEndian.PutUint64(buf[offset:offset+8], cms.table[i][j])
		}
	}
	return buf, nil
}

func Deserialize(data []byte) (*CountMinSketch, error) {
	if len(data) < 12 {
		return nil, ErrInvalidCMSData
	}

	m := uint(binary.BigEndian.Uint32(data[0:4]))
	k := uint(binary.BigEndian.Uint32(data[4:8]))
	seed := binary.BigEndian.Uint32(data[8:12])

	// Provera dužine
	expectedLen := 12 + int(k)*int(m)*8
	if len(data) != expectedLen {
		return nil, ErrInvalidCMSData
	}

	table := make([][]uint64, k)
	for i := uint(0); i < k; i++ {
		table[i] = make([]uint64, m)
		for j := uint(0); j < m; j++ {
			offset := 12 + int(i*m+j)*8
			table[i][j] = binary.BigEndian.Uint64(data[offset : offset+8])
		}
	}

	return &CountMinSketch{
		m:      m,
		k:      k,
		seed:   seed,
		table:  table,
		hashes: CreateHashFunctionsWithSeed(uint32(k), seed),
	}, nil
}

func Merge(ops []model.Record, epsilon float64, delta float64) *CountMinSketch {
	cms := NewCountMinSketch(epsilon, delta)

	for _, rec := range ops {
		if rec.Op == model.MergeOpAdd {
			cms.Add([]byte(rec.Key))
		}
	}
	return cms
}

func (cms *CountMinSketch) Reset() {
	for i := range cms.table {
		for j := range cms.table[i] {
			cms.table[i][j] = 0
		}
	}
}
