package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"kv-engine/internal/block"
	"os"
)

type WALSegmentHeader struct {
	Magic         [4]byte
	BlockSize     uint32
	SegmentBlocks uint32
}

const WALSegmentHeaderSize = 4 + 4 + 4 // Magic (4 bytes) + BlockSize (4 bytes) + SegmentBlocks (4 bytes)

func NewWalSegmentHeader(blockSize uint32, SegmentBlocks uint32) *WALSegmentHeader {
	return &WALSegmentHeader{
		Magic:         [4]byte{'W', 'A', 'L', 'S'},
		BlockSize:     blockSize,
		SegmentBlocks: SegmentBlocks,
	}
}

func (h *WALSegmentHeader) Serialize() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, h.Magic)
	binary.Write(buffer, binary.LittleEndian, h.BlockSize)
	binary.Write(buffer, binary.LittleEndian, h.SegmentBlocks)
	return buffer.Bytes()
}

func DeserializeWALSegmentHeader(data []byte) (*WALSegmentHeader, error) {
	if len(data) != WALSegmentHeaderSize {
		return nil, fmt.Errorf("not enough bytes to read WALSegmentHeader, got %d, need %d", len(data), WALSegmentHeaderSize)
	}

	reader := bytes.NewReader(data)
	var header WALSegmentHeader

	// Čitanje polja headera po redu, LittleEndian
	if err := binary.Read(reader, binary.LittleEndian, &header.Magic); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.BlockSize); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.SegmentBlocks); err != nil {
		return nil, err
	}

	// Opcionalno: validacija Magic polja
	if string(header.Magic[:]) != "WALS" {
		return nil, fmt.Errorf("invalid WAL segment magic: %v", header.Magic)
	}

	return &header, nil
}

type WALSegment struct {
	File             *os.File
	FilePath         string
	Header           *WALSegmentHeader
	CurrentBlock     int
	RemainingInBlock int
}

func NewWALSegment(filePath string, header *WALSegmentHeader, bm *block.BlockManager) (*WALSegment, error) {
	if int(header.BlockSize) < WALSegmentHeaderSize {
		return nil, fmt.Errorf("block size %d is smaller than WAL segment header size %d", header.BlockSize, WALSegmentHeaderSize)
	}

	blockSize := int(header.BlockSize)
	headerData := header.Serialize()
	blockData := make([]byte, blockSize)
	copy(blockData, headerData)

	if err := bm.WriteBlock(filePath, 0, blockData, blockSize); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WALSegment{
		File:             file,
		FilePath:         filePath,
		Header:           header,
		CurrentBlock:     0,
		RemainingInBlock: blockSize - WALSegmentHeaderSize,
	}, nil
}
