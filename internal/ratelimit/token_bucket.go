package ratelimit

import (
	"encoding/binary"
	"fmt"
	"time"
)

const stateSize = 16

type Snapshot struct {
	Tokens             int64
	LastRefillUnixNano int64
}

type TokenBucket struct {
	capacity           int64
	refillInterval     time.Duration
	tokens             int64
	lastRefillUnixNano int64
}

func New(capacity int64, refillInterval time.Duration, now time.Time) (*TokenBucket, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("token bucket capacity must be > 0")
	}
	if refillInterval <= 0 {
		return nil, fmt.Errorf("token bucket refill interval must be > 0")
	}
	return &TokenBucket{
		capacity:           capacity,
		refillInterval:     refillInterval,
		tokens:             capacity,
		lastRefillUnixNano: now.UnixNano(),
	}, nil
}

func (b *TokenBucket) TryConsume(now time.Time) bool {
	return b.TryConsumeN(now, 1)
}

func (b *TokenBucket) TryConsumeN(now time.Time, n int64) bool {
	if n <= 0 {
		return true
	}
	b.refill(now.UnixNano())
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

func (b *TokenBucket) Snapshot() Snapshot {
	return Snapshot{
		Tokens:             b.tokens,
		LastRefillUnixNano: b.lastRefillUnixNano,
	}
}

func (b *TokenBucket) Restore(s Snapshot) {
	b.tokens = clamp(s.Tokens, 0, b.capacity)
	b.lastRefillUnixNano = s.LastRefillUnixNano
}

func (b *TokenBucket) MarshalBinary() []byte {
	out := make([]byte, stateSize)
	binary.LittleEndian.PutUint64(out[0:8], uint64(b.tokens))
	binary.LittleEndian.PutUint64(out[8:16], uint64(b.lastRefillUnixNano))
	return out
}

func (b *TokenBucket) UnmarshalBinary(data []byte) error {
	if len(data) != stateSize {
		return fmt.Errorf("invalid token bucket state size: got=%d want=%d", len(data), stateSize)
	}
	s := Snapshot{
		Tokens:             int64(binary.LittleEndian.Uint64(data[0:8])),
		LastRefillUnixNano: int64(binary.LittleEndian.Uint64(data[8:16])),
	}
	b.Restore(s)
	return nil
}

func (b *TokenBucket) refill(nowUnixNano int64) {
	if nowUnixNano <= b.lastRefillUnixNano {
		return
	}
	elapsed := time.Duration(nowUnixNano - b.lastRefillUnixNano)
	if elapsed < b.refillInterval {
		return
	}

	steps := int64(elapsed / b.refillInterval)
	if steps <= 0 {
		return
	}

	b.tokens = clamp(b.tokens+steps, 0, b.capacity)
	b.lastRefillUnixNano += int64(time.Duration(steps) * b.refillInterval)
}

func clamp(v, minV, maxV int64) int64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}
