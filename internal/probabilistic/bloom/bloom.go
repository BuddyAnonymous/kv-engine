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
	bitset []byte         // bitset
	hashes []HashWithSeed // hash funkcije
	mutex  sync.RWMutex   // thread safety
}

// Konstruktor
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)

	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, (m+7)/8),
		hashes: CreateHashFunctions(uint32(k)),
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
// [8:]  -> bitset
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

	buf := make([]byte, 8+len(bf.bitset))
	binary.BigEndian.PutUint32(buf[0:4], uint32(bf.m))
	binary.BigEndian.PutUint32(buf[4:8], uint32(bf.k))
	copy(buf[8:], bf.bitset)

	return buf, nil
}

// DeserializeBloomFilter kreira Bloom filter iz bajtova
func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, ErrInvalidBloomData
	}

	m := uint(binary.BigEndian.Uint32(data[0:4]))
	k := uint(binary.BigEndian.Uint32(data[4:8]))

	if m == 0 || k == 0 {
		return nil, ErrInvalidBloomData
	}

	expectedLen := int((m + 7) / 8)
	if len(data[8:]) != expectedLen {
		return nil, ErrInvalidBloomData
	}

	bitset := make([]byte, expectedLen)
	copy(bitset, data[8:])

	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: bitset,
		hashes: CreateHashFunctions(uint32(k)),
	}, nil
}
