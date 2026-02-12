package bloom

import (
	"encoding/binary"
	"errors"
	"sync"
)

var (
	ErrInvalidBloomData = errors.New("invalid bloom filter data")
)

type BloomFilter struct {
	m      uint           // broj bitova
	k      uint           // broj hash funkcija
	seed   uint32         // seed za hash funkcije
	bitset []byte         // bitset
	hashes []HashWithSeed // hash funkcije
	mutex  sync.RWMutex   // thread safety
}

// Konstruktor
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	hashes, seed := CreateHashFunctions(uint32(k))

	return &BloomFilter{
		m:      m,
		k:      k,
		seed:   seed,
		bitset: make([]byte, (m+7)/8),
		hashes: hashes,
	}
}

// Add dodaje element u Bloom filter
func (bf *BloomFilter) Add(data []byte) {
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

// MightContain proverava da li element mozda postoji
func (bf *BloomFilter) MightContain(data []byte) bool {
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

// Serialize serijalizuje Bloom filter
// Format:
// [0:4] -> m
// [4:8] -> k
// [8:12] -> seed
// [12:]  -> bitset
func (bf *BloomFilter) Serialize() ([]byte, error) {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()

	if bf.m == 0 || bf.k == 0 {
		return nil, ErrInvalidBloomData
	}

	expectedLen := int((bf.m + 7) / 8)
	if len(bf.bitset) != expectedLen {
		return nil, ErrInvalidBloomData
	}

	buf := make([]byte, 12+len(bf.bitset))
	binary.BigEndian.PutUint32(buf[0:4], uint32(bf.m))
	binary.BigEndian.PutUint32(buf[4:8], uint32(bf.k))
	binary.BigEndian.PutUint32(buf[8:12], bf.seed)
	copy(buf[12:], bf.bitset)

	return buf, nil
}

// Deserialize BloomFiltera kreira Bloom filter iz bajtova
func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 12 {
		return nil, ErrInvalidBloomData
	}

	m := uint(binary.BigEndian.Uint32(data[0:4]))
	k := uint(binary.BigEndian.Uint32(data[4:8]))
	seed := binary.BigEndian.Uint32(data[8:12])

	if m == 0 || k == 0 {
		return nil, ErrInvalidBloomData
	}

	expectedLen := int((m + 7) / 8)
	if len(data[12:]) != expectedLen {
		return nil, ErrInvalidBloomData
	}

	bitset := make([]byte, expectedLen)
	copy(bitset, data[12:])

	return &BloomFilter{
		m:      m,
		k:      k,
		seed:   seed,
		bitset: bitset,
		hashes: CreateHashFunctionsWithSeed(uint32(k), seed),
	}, nil
}
