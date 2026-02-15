package sstable

import (
	"bytes"
	"encoding/binary"

	"kv-engine/internal/model"
)

const (
	fragTypeMask   byte = 0b00000011
	tombstoneMask  byte = 0b00000100
	kindMask       byte = 0b00001000
	structureShift uint = 4
	opShift        uint = 6
	structureMask  byte = 0b00110000
	opMask         byte = 0b11000000
)

// header = 8 bytes: MAGIC(4) + BLOCKSIZE(u16) + FLAGS(u16)
func (m *Manager) encodeHeader(magic [4]byte) []byte {
	h := make([]byte, 8)
	copy(h[0:4], magic[:])
	binary.LittleEndian.PutUint16(h[4:6], uint16(m.blockSize))
	binary.LittleEndian.PutUint16(h[6:8], uint16(m.flags))
	return h
}

// encodeDataRecord creates encoded bytes for one data record.
func (m *Manager) encodeDataRecord(prevKey string, r model.Record) ([]byte, string, error) {
	key := r.Key

	shared := 0
	if prevKey != "" {
		shared = sharedPrefixLen(prevKey, key)
	}
	suffix := key[shared:]

	expiresAt := r.ExpiresAt

	val := r.Value
	if r.Tombstone {
		val = nil
	}
	valLen := uint64(len(val))

	recFlags := encodeRecordFlags(r)

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
	buf.Write(uvarintBytes(dataBlockNo))
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
	buf.Write(uvarintBytes(indexBlockNo))
	return buf.Bytes(), key
}

func encodeDataHeader(prevKey string, r model.Record) (hdr []byte, err error) {
	var buf bytes.Buffer

	recFlags := encodeRecordFlags(r)
	shared := sharedPrefixLen(prevKey, r.Key)
	suffix := r.Key[shared:]
	valLen := uint64(len(r.Value))
	if r.Tombstone {
		valLen = 0
	}

	buf.WriteByte(recFlags)

	expiresAt := r.ExpiresAt
	buf.Write(uvarintBytes(expiresAt))
	buf.Write(uvarintBytes(uint64(shared)))
	buf.Write(uvarintBytes(uint64(len(suffix))))
	buf.WriteString(suffix)
	buf.Write(uvarintBytes(r.Seq))
	buf.Write(uvarintBytes(valLen))

	return buf.Bytes(), nil
}

func encodeRecordFlags(r model.Record) byte {
	var flags byte
	if r.Tombstone {
		flags |= tombstoneMask
	}
	if r.Kind == model.RecordKindMergeOperand {
		flags |= kindMask
	}
	flags |= byte(r.Structure&0b11) << structureShift
	flags |= byte(r.Op&0b11) << opShift
	return flags
}
