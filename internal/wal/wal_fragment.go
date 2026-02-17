package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type FragmentType byte

const (
	FragmentFull    FragmentType = 0 // ceo zapis staje u jedan fragment
	FragmentFirst   FragmentType = 1
	FragmentMiddle  FragmentType = 2
	FragmentLast    FragmentType = 3
	FragmentPadding FragmentType = 4
)

const WALFragmentHeaderSize = 4 + 1 + 4 // CRC32 (4 bytes) + Type (1 byte) + DataLen (4 bytes)

type WALFragment struct {
	CRC32   uint32
	Type    FragmentType
	DataLen uint32
	Data    []byte
}

func NewWALFragment(Type FragmentType, DataLen uint32, Data []byte) *WALFragment {
	return &WALFragment{
		CRC32:   crc32.ChecksumIEEE(Data),
		Type:    Type,
		DataLen: DataLen,
		Data:    Data,
	}
}

func (f *WALFragment) Serialize() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, f.CRC32)
	binary.Write(buffer, binary.LittleEndian, byte(f.Type))
	binary.Write(buffer, binary.LittleEndian, f.DataLen)
	buffer.Write(f.Data)
	return buffer.Bytes()
}

// Vraća CRC32, Type i DataLen, ili grešku ako je niz pogrešne veličine
func DeserializeWALFragmentHeader(data []byte) (uint32, FragmentType, uint32, error) {
	if len(data) < WALFragmentHeaderSize {
		for _, b := range data {
			if b != 0 {
				return 0, 0, 0, fmt.Errorf("invalid WAL fragment header size: expected at least %d bytes, got %d", WALFragmentHeaderSize, len(data))
			}
		}
		return 0, FragmentPadding, 0, nil
	}
	if len(data) != WALFragmentHeaderSize {
		return 0, 0, 0, fmt.Errorf("invalid WAL fragment header size: expected %d bytes, got %d", WALFragmentHeaderSize, len(data))
	}

	reader := bytes.NewReader(data)

	var crc32Val uint32
	var fragType byte
	var dataLen uint32

	if err := binary.Read(reader, binary.LittleEndian, &crc32Val); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read CRC32: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &fragType); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read fragment type: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &dataLen); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read data length: %w", err)
	}

	return crc32Val, FragmentType(fragType), dataLen, nil
}
