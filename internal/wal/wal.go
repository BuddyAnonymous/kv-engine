package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

const (
	OpDelete byte = 0
	OpPut    byte = 1
)

type FragmentType byte

const (
	FragmentFull   FragmentType = 0 // ceo zapis staje u jedan fragment
	FragmentFirst  FragmentType = 1
	FragmentMiddle FragmentType = 2
	FragmentLast   FragmentType = 3
)

type WALFragment struct {
	Type    FragmentType
	DataLen uint32
	Data    []byte
}

func (f *WALFragment) NewWALFregment(Type FragmentType, DataLen uint32, Data []byte) *WALFragment {
	return &WALFragment{
		Type:    Type,
		DataLen: DataLen,
		Data:    Data,
	}
}

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

type WALSegmentHeader struct {
	Magic         [4]byte
	BlockSize     int
	SegmentBlocks int
}

type WALSegment struct {
	File     *os.File
	FilePath string
}

type WALManager struct {
	DirPath        string
	MaxSegmentSize int64
	CurrentSegment *WALSegment
	SegmentID      int
}
