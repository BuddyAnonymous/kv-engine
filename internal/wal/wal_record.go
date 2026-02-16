package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	OpDelete byte = 0
	OpPut    byte = 1
)

type WALRecord struct {
	CRC32     uint32
	Seq       uint64
	ExpiresAt uint64
	OpType    byte // 0 = DELETE, 1 = PUT
	KeyLen    uint32
	ValueLen  uint32
	Key       []byte
	Value     []byte
}

// Binarizuje Wal zapis
func (r *WALRecord) Serialize() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, r.CRC32)
	binary.Write(buffer, binary.LittleEndian, r.Seq)
	binary.Write(buffer, binary.LittleEndian, r.ExpiresAt)
	binary.Write(buffer, binary.LittleEndian, r.OpType)
	binary.Write(buffer, binary.LittleEndian, r.KeyLen)
	binary.Write(buffer, binary.LittleEndian, r.ValueLen)
	buffer.Write(r.Key)
	buffer.Write(r.Value)
	return buffer.Bytes()
}

// Konstruktor za ceo WAL zapis
func NewWALRecord(seq uint64, expiresAt uint64, opType byte, key []byte, value []byte) *WALRecord {
	wal := WALRecord{
		CRC32:     0, // Racuna se kada se wal upisuje na disk
		Seq:       seq,
		ExpiresAt: expiresAt,
		OpType:    opType,
		KeyLen:    uint32(len(key)),
		ValueLen:  uint32(len(value)),
		Key:       key,
		Value:     value,
	}
	// Izracunavanje CRC32
	serialized := wal.Serialize()
	wal.CRC32 = crc32.ChecksumIEEE(serialized[4:])
	return &wal
}

// DeserializeWALRecord deserijalizuje WAL zapis iz niza bajtova
func DeserializeWALRecord(data []byte) (*WALRecord, error) {
	// Minimalna veličina: 4 + 8 + 8 + 1 + 4 + 4 = 29 bajtova
	const minHeaderSize = 4 + 8 + 8 + 1 + 4 + 4

	if len(data) < minHeaderSize {
		return nil, fmt.Errorf("insufficient data for WAL record: expected at least %d bytes, got %d", minHeaderSize, len(data))
	}

	reader := bytes.NewReader(data)

	var crc32Val uint32
	var seq uint64
	var expiresAt uint64
	var opType byte
	var keyLen uint32
	var valueLen uint32

	// Čitaj header
	if err := binary.Read(reader, binary.LittleEndian, &crc32Val); err != nil {
		return nil, fmt.Errorf("failed to read CRC32: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &seq); err != nil {
		return nil, fmt.Errorf("failed to read Seq: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &expiresAt); err != nil {
		return nil, fmt.Errorf("failed to read ExpiresAt: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &opType); err != nil {
		return nil, fmt.Errorf("failed to read OpType: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read KeyLen: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, fmt.Errorf("failed to read ValueLen: %w", err)
	}

	// Čitaj Key
	key := make([]byte, keyLen)
	if err := binary.Read(reader, binary.LittleEndian, &key); err != nil {
		return nil, fmt.Errorf("failed to read Key: %w", err)
	}

	// Čitaj Value
	value := make([]byte, valueLen)
	if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
		return nil, fmt.Errorf("failed to read Value: %w", err)
	}

	// Kreiraj WALRecord
	record := NewWALRecord(seq, expiresAt, opType, key, value)

	// Validacija CRC32 - validiraj sa svakim delom osim prvog CRC32 polja

	if record.CRC32 != crc32Val {
		return nil, fmt.Errorf("CRC32 validation failed: expected 0x%x, got 0x%x", record.CRC32, crc32Val)
	}

	return record, nil
}
