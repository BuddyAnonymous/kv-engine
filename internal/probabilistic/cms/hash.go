package cms

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint) ([]HashWithSeed, uint32) {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h, uint32(ts)
}

func CreateHashFunctionsWithSeed(k uint32, baseSeed uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)

	for i := uint32(0); i < k; i++ {
		seedBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(seedBytes, baseSeed+i)

		h[i] = HashWithSeed{Seed: seedBytes}
	}

	return h
}
