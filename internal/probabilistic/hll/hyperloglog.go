package hll

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"kv-engine/internal/model"
	"math"
	"math/bits"
	"time"
)

var (
	ErrInvalidHLLMeta = errors.New("invalid hll meta")
	ErrInvalidHLLData = errors.New("invalid hll data")
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// HLL struktura
type HLL struct {
	p    uint8   // preciznost (4-16)
	m    uint64  // broj registara = 2^p
	seed uint32  // seed za hash funkciju
	reg  []uint8 // registri
}

// Konstruktor
func NewHLL(p uint8) (*HLL, error) {
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil, ErrInvalidHLLMeta
	}

	m := uint64(1) << p
	reg := make([]uint8, m)

	seed := uint32(time.Now().UnixNano())

	return &HLL{
		p:    p,
		m:    m,
		seed: seed,
		reg:  reg,
	}, nil
}

// NewFromMeta - za kreiranje iz meta bloka (sa fiksnim seed-om)
func NewFromMeta(p uint8, seed uint32) *HLL {
	m := uint64(1) << p
	return &HLL{
		p:    p,
		m:    m,
		seed: seed,
		reg:  make([]uint8, m),
	}
}

// hash funkcija
func (hll *HLL) hash(data []byte) uint64 {
	hasher := fnv.New64a()

	// Prvo upisujemo seed (kao dodatni bajtovi)
	seedBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(seedBytes, hll.seed)
	hasher.Write(seedBytes)

	// Onda podaci
	hasher.Write(data)
	return hasher.Sum64()
}

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

// Add dodaje element u HLL
func (hll *HLL) Add(data []byte) {
	hash := hll.hash(data)

	// Prvih p bitova - indeks registra
	idx := firstKbits(hash, uint64(hll.p))

	// Ostali bitovi - za racunanje vodecih nula
	w := hash << hll.p
	rank := trailingZeroBits(w) + 1

	if uint8(rank) > hll.reg[idx] {
		hll.reg[idx] = uint8(rank)
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// Format:
// [0]    -> p (1 bajt)
// [1:9] -> m (8 bajtova)
// [9:13]  -> seed (4 bajta)
// [13:]  -> registri (m bajtova)
func (hll *HLL) Serialize() ([]byte, error) {
	if hll.p == 0 || hll.m == 0 {
		return nil, ErrInvalidHLLData
	}

	if uint64(len(hll.reg)) != hll.m {
		return nil, ErrInvalidHLLData
	}

	buf := make([]byte, 1+8+4+len(hll.reg))

	buf[0] = hll.p
	binary.BigEndian.PutUint64(buf[1:9], hll.m)
	binary.BigEndian.PutUint32(buf[9:13], hll.seed)
	copy(buf[13:], hll.reg)

	return buf, nil
}

// Deserialize kreira HLL iz bajtova
func Deserialize(data []byte) (*HLL, error) {
	if len(data) < 13 {
		return nil, ErrInvalidHLLData
	}

	p := data[0]
	m := binary.BigEndian.Uint64(data[1:9])
	seed := binary.BigEndian.Uint32(data[9:13])

	// Provera da li p ima dozvoljenu vrednost
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil, ErrInvalidHLLMeta
	}

	// Provera da li m odgovara p
	if m != (uint64(1) << p) {
		return nil, ErrInvalidHLLData
	}

	expectedLen := 13 + int(m)
	if len(data) < expectedLen {
		return nil, ErrInvalidHLLData
	}

	reg := make([]uint8, m)
	copy(reg, data[13:13+m])

	return &HLL{
		p:    p,
		m:    m,
		seed: seed,
		reg:  reg,
	}, nil
}

func Merge(ops []model.Record, p uint8, seed uint32) *HLL {
	hll := NewFromMeta(p, seed)

	for _, rec := range ops {
		if rec.Op == model.MergeOpAdd {
			hll.Add(rec.Value)
		}
	}
	return hll
}

func (hll *HLL) Reset() {
	for i := range hll.reg {
		hll.reg[i] = 0
	}
}
