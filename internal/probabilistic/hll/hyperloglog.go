package hll

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidHLLMeta = errors.New("invalid hll meta")
)

// struktura iz helpera
type HLL struct {
	m   uint64  // broj registara = 2^p
	p   uint8   // preciznost
	reg []uint8 // registri
}

// Konstruktor
func NewHLL(p uint8) (*HLL, error) {
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil, ErrInvalidHLLMeta
	}

	m := uint64(1) << p
	reg := make([]uint8, m)

	return &HLL{
		m:   m,
		p:   p,
		reg: reg,
	}, nil
}

func (hll *HLL) Add(hash uint64) {

	idx := firstKbits(hash, uint64(hll.p))
	w := hash << hll.p
	rank := trailingZeroBits(w) + 1

	if uint8(rank) > hll.reg[idx] {
		hll.reg[idx] = uint8(rank)
	}
}

// Merge dve HLL strukture (kompakcije)
func (hll *HLL) Merge(other *HLL) error {
	if hll.p != other.p || hll.m != other.m {
		return ErrInvalidHLLMeta
	}

	for i := range hll.reg {
		if other.reg[i] > hll.reg[i] {
			hll.reg[i] = other.reg[i]
		}
	}
	return nil
}

// Format:
// [0] -> p
// [1:9] -> m
// [9:] -> reg
func (hll *HLL) Serialize() ([]byte, error) {

	buf := make([]byte, 1+8+len(hll.reg))
	buf[0] = hll.p
	binary.BigEndian.PutUint64(buf[1:9], hll.m)
	copy(buf[9:], hll.reg)

	return buf, nil
}

func Deserialize(data []byte) (*HLL, error) {
	if len(data) < 9 {
		return nil, ErrInvalidHLLMeta
	}

	p := data[0]
	m := binary.BigEndian.Uint64(data[1:9])

	if m != (uint64(1) << p) {
		return nil, ErrInvalidHLLMeta
	}

	reg := make([]uint8, m)
	copy(reg, data[9:])

	return &HLL{
		p:   p,
		m:   m,
		reg: reg,
	}, nil
}
