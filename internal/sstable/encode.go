package sstable

import (
	"bytes"
	"encoding/binary"
	"kv-engine/internal/model"
)

// header = 8 bytes: MAGIC(4) + BLOCKSIZE(u16) + FLAGS(u16)
func (m *Manager) encodeHeader(magic [4]byte) []byte {
	h := make([]byte, 8)
	copy(h[0:4], magic[:])
	binary.LittleEndian.PutUint16(h[4:6], uint16(m.blockSize))
	binary.LittleEndian.PutUint16(h[6:8], uint16(m.flags))
	return h
}

// encodeDataRecord pravi bytes za jedan record.
func (m *Manager) encodeDataRecord(prevKey string, r model.Record) ([]byte, string, error) {
	key := r.Key

	shared := 0
	if prevKey != "" {
		shared = sharedPrefixLen(prevKey, key)
	}
	suffix := key[shared:]

	// --- TTL / expiresAt ---
	// Ako nema TTL: expiresAt = 0
	var expiresAt uint64 = r.ExpiresAt

	// --- Value ---
	val := r.Value
	if r.Tombstone {
		val = nil
	}
	valLen := uint64(len(val))

	// --- RecFlags ---
	// FULL (npr. 0b00) ako fragLen==0, inače FIRST/MIDDLE/LAST (npr. 0b01/10/11)
	// FULL = 0 (poslednja 2 bita 00), a tombstone bit na (bit2 od pozadi).
	var recFlags byte = 0
	if r.Tombstone {
		// "3. bit od nazad" = bit2 , tj maska 0b00000100
		recFlags |= 0b00000100
	}

	var buf bytes.Buffer
	buf.WriteByte(recFlags)
	buf.Write(uvarintBytes(expiresAt))
	buf.Write(uvarintBytes(uint64(shared)))
	buf.Write(uvarintBytes(uint64(len(suffix))))
	buf.WriteString(suffix)
	buf.Write(uvarintBytes(r.Seq))
	buf.Write(uvarintBytes(valLen))
	buf.Write(val)

	return buf.Bytes(), key, nil
}

func encodeIndexEntry(prevKey, key string, dataBlockNo uint64) ([]byte, string) {
	shared := 0
	if prevKey != "" {
		shared = sharedPrefixLen(prevKey, key)
	}
	suffix := key[shared:]

	var buf bytes.Buffer
	buf.Write(uvarintBytes(uint64(shared)))
	buf.Write(uvarintBytes(uint64(len(suffix))))
	buf.WriteString(suffix)
	buf.Write(uvarintBytes(dataBlockNo)) // DATABLOCKOFFSET = blockNumber
	return buf.Bytes(), key
}

func encodeSummaryEntry(prevKey, key string, indexBlockNo uint64) ([]byte, string) {
	shared := 0
	if prevKey != "" {
		shared = sharedPrefixLen(prevKey, key)
	}
	suffix := key[shared:]

	var buf bytes.Buffer
	buf.Write(uvarintBytes(uint64(shared)))
	buf.Write(uvarintBytes(uint64(len(suffix))))
	buf.WriteString(suffix)
	buf.Write(uvarintBytes(indexBlockNo)) // INDEXOFFSET = blockNumber u index fajlu
	return buf.Bytes(), key
}

func encodeDataHeader(prevKey string, r model.Record) (hdr []byte, err error) {
	var buf bytes.Buffer

	recFlags := byte(0)
	if r.Tombstone {
		recFlags |= (1 << 2)
	}
	shared := sharedPrefixLen(prevKey, r.Key)
	suffix := r.Key[shared:]

	buf.WriteByte(recFlags)

	expiresAt := r.ExpiresAt
	buf.Write(uvarintBytes(expiresAt))
	buf.Write(uvarintBytes(uint64(shared)))
	buf.Write(uvarintBytes(uint64(len(suffix))))
	buf.WriteString(suffix)
	buf.Write(uvarintBytes(r.Seq))
	buf.Write(uvarintBytes(uint64(len(r.Value)))) // valLen

	return buf.Bytes(), nil
}
