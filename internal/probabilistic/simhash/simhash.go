package simhash

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math/bits"
	"strings"
)

var (
	ErrInvalidSimHashData = errors.New("invalid SimHash data")
)

type SimHash struct {
	vec [64]int // vektor za glasanje (za kreiranje fingerprint-a, ne serializuje se)
	fp  uint64  // fingerprint
}

// NewSimHash kreira SimHash od teksta
func NewSimHash(text string) *SimHash {
	sh := &SimHash{}

	// Tokenizacija - podeli tekst na reci
	words := strings.Fields(text)

	for _, word := range words {
		h := fnv.New64a()
		h.Write([]byte(word))
		hash := h.Sum64()

		// Glasanje za svaki od 64 bita
		for i := 0; i < 64; i++ {
			// Proveravamo da li je i-ti bit postavljen na 1
			if (hash>>uint(i))&1 == 1 {
				sh.vec[i]++ // Glas ZA (+1)
			} else {
				sh.vec[i]-- // Glas PROTIV (-1)
			}
		}
	}

	// Pretvaranje vektora u fingerprint
	var fp uint64
	for i := 0; i < 64; i++ {
		if sh.vec[i] > 0 {
			fp |= 1 << uint(i)
		}
		// Ako je <= 0, bit ostaje 0
	}
	sh.fp = fp

	return sh
}

func (sh *SimHash) Fingerprint() uint64 {
	return sh.fp
}

// HammingDistance racuna broj bitova u kojima se dva fingerprint-a razlikuju
func HammingDistance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

// Similarity racuna slicnost (0.0 - 1.0)
func Similarity(a, b uint64) float64 {
	return 1.0 - float64(HammingDistance(a, b))/64.0
}

// Format
// [0:8] -> fingerprint
func (sh *SimHash) Serialize() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, sh.fp)
	return buf, nil
}

func Deserialize(data []byte) (*SimHash, error) {
	if len(data) < 8 {
		return nil, ErrInvalidSimHashData
	}

	fp := binary.BigEndian.Uint64(data[:8])
	return &SimHash{fp: fp}, nil
}

// CalculateForTwoStrings računa sličnost između dva stringa
func CalculateForTwoStrings(str1, str2 string) float64 {
	sh1 := NewSimHash(str1)
	sh2 := NewSimHash(str2)
	return Similarity(sh1.Fingerprint(), sh2.Fingerprint())
}
