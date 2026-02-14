package cms

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type CountMinSketch struct {
	m      uint
	k      uint
	table  [][]uint64
	hashes []HashWithSeed
}

func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)

	table := make([][]uint64, k)
	for i := uint(0); i < k; i++ {
		table[i] = make([]uint64, m)
	}

	hashes := CreateHashFunctions(k)

	return &CountMinSketch{
		m:      m,
		k:      k,
		table:  table,
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
	if cms.m != other.m || cms.k != other.k {
		return errors.New("CMS parametri se ne poklapaju")
	}

	for i := uint(0); i < cms.k; i++ {
		for j := uint(0); j < cms.m; j++ {
			cms.table[i][j] += other.table[i][j]
		}
	}
	return nil
}

func (cms *CountMinSketch) Serialize() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, uint64(cms.m))
	binary.Write(buf, binary.BigEndian, uint64(cms.k))

	for i := uint(0); i < cms.k; i++ {
		for j := uint(0); j < cms.m; j++ {
			binary.Write(buf, binary.BigEndian, cms.table[i][j])
		}
	}
	return buf.Bytes()
}

func DeserializeCMS(data []byte) (*CountMinSketch, error) {
	buf := bytes.NewReader(data)

	var m, k uint64
	if err := binary.Read(buf, binary.BigEndian, &m); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &k); err != nil {
		return nil, err
	}

	table := make([][]uint64, k)
	for i := uint64(0); i < k; i++ {
		table[i] = make([]uint64, m)
		for j := uint64(0); j < m; j++ {
			if err := binary.Read(buf, binary.BigEndian, &table[i][j]); err != nil {
				return nil, err
			}
		}
	}

	hashes := CreateHashFunctions(uint(k))

	return &CountMinSketch{
		m:      uint(m),
		k:      uint(k),
		table:  table,
		hashes: hashes,
	}, nil
}
