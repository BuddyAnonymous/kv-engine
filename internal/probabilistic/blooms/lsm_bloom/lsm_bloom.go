package lsm_bloom

import (
	"encoding/binary"
	"errors"
	"sync"

	"kv-engine/internal/probabilistic/blooms"
)

var (
	ErrInvalidBloomMeta  = errors.New("invalid bloom meta")
	ErrInvalidBloomDelta = errors.New("invalid bloom delta")
)

type LSMBloom struct {
	m      uint
	k      uint
	seed   uint32
	bitset []byte
	hashes []blooms.HashWithSeed
	mutex  sync.RWMutex
}

func NewFromMeta(m uint, k uint, seed uint32) *LSMBloom {
	hashes := blooms.CreateHashFunctionsWithSeed(uint32(k), seed)

	return &LSMBloom{
		m:      m,
		k:      k,
		seed:   seed,
		bitset: make([]byte, (m+7)/8),
		hashes: hashes,
	}
}

func (bf *LSMBloom) ApplyAdd(data []byte) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	for _, h := range bf.hashes {
		hash := h.Hash(data)
		index := hash % uint64(bf.m)

		byteIndex := index / 8
		bitIndex := index % 8

		bf.bitset[byteIndex] |= 1 << bitIndex
	}
}

func (bf *LSMBloom) MightContain(data []byte) bool {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	for _, h := range bf.hashes {
		hash := h.Hash(data)
		index := hash % uint64(bf.m)

		byteIndex := index / 8
		bitIndex := index % 8

		if (bf.bitset[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

func (bf *LSMBloom) Merge(other *LSMBloom) error {
	if bf.m != other.m || bf.k != other.k || bf.seed != other.seed {
		return ErrInvalidBloomMeta
	}

	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	for i := range bf.bitset {
		bf.bitset[i] |= other.bitset[i]
	}
	return nil
}

func (bf *LSMBloom) SerializeMeta() []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bf.m))
	binary.BigEndian.PutUint32(buf[4:8], uint32(bf.k))
	binary.BigEndian.PutUint32(buf[8:12], bf.seed)
	return buf
}

func DeserializeMeta(data []byte) (*LSMBloom, error) {
	if len(data) != 12 {
		return nil, ErrInvalidBloomMeta
	}

	m := uint(binary.BigEndian.Uint32(data[0:4]))
	k := uint(binary.BigEndian.Uint32(data[4:8]))
	seed := binary.BigEndian.Uint32(data[8:12])

	if m == 0 || k == 0 {
		return nil, ErrInvalidBloomMeta
	}

	return NewFromMeta(m, k, seed), nil
}

func SerializeAddOperation(data []byte) []byte {
	return data
}

func DeserializeAddOperation(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, ErrInvalidBloomDelta
	}
	return data, nil
}
