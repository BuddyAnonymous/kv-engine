package wal

import (
	"fmt"
	"hash/crc32"
	"kv-engine/internal/block"
	"kv-engine/internal/model"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RecordApplier je interfejs za primenu WAL zapisa (razbija ciklicni import sa engine).
type RecordApplier interface {
	ApplyRecord(rec model.Record, fromWAL bool) error
}

type WALManager struct {
	DirPath          string
	MaxSegmentBlocks int
	BlockSize        int
	CurrentSegment   *WALSegment
	SegmentID        int
	FirstSegmentID   int
	bm               *block.BlockManager
	applier          RecordApplier
}

func NewWALManager(dirpath string, configMaxSegmentBlocks int, configBlockSize int, bm *block.BlockManager, applier RecordApplier) (*WALManager, error, uint64) {
	// Kreiraj direktorijum ako ne postoji
	if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
		return nil, err, 0
	}

	files, err := os.ReadDir(dirpath)
	if err != nil {
		return nil, err, 0
	}

	manager := &WALManager{
		DirPath:          dirpath,
		BlockSize:        configBlockSize,
		MaxSegmentBlocks: configMaxSegmentBlocks,
		bm:               bm,
		applier:          applier,
	}
	// SCENARIO 1: WAL NE POSTOJI
	if len(files) == 0 {
		manager.SegmentID = 0
		if err := manager.rotateSegment(bm); err != nil {
			return nil, err, 0
		}
		return manager, nil, 0
	}

	// SCENARIO 2: POSTOJECI WAL
	firstID := -1
	lastID := -1
	var lastSegmentPath string

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		id := parseSegmentID(f.Name())
		if id < 0 {
			continue
		}
		if firstID == -1 || id < firstID {
			firstID = id
		}
		if id > lastID {
			lastID = id
			lastSegmentPath = filepath.Join(dirpath, f.Name())
		}
	}

	if lastID == -1 {
		return nil, fmt.Errorf("no valid WAL segments found in %s", dirpath), 0
	}

	manager.SegmentID = lastID
	manager.FirstSegmentID = firstID

	// Otvori poslednji segment
	file, err := os.OpenFile(lastSegmentPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err, 0
	}

	// Procitaj header poslednjeg segmenta
	headerBytes, err := bm.ReadAt(lastSegmentPath, 0, WALSegmentHeaderSize)
	if err != nil {
		file.Close()
		return nil, err, 0
	}

	header, err := DeserializeWALSegmentHeader(headerBytes)
	if err != nil {
		file.Close()
		return nil, err, 0
	}

	// Reprodukcija WAL-a od prvog do poslednjeg segmenta, i pronalazak trenutne pozicije u poslednjem segmentu
	currentBlock, currentOffset, err, lastSeq := ReplayWAL(firstID, lastID, dirpath, bm, applier)
	if err != nil {
		file.Close()
		return nil, err, 0
	}
	// Kreiranje novog segmenta ako je poslednji segment pun
	if currentBlock == 0 && currentOffset == 0 {
		// Poslednji segment je pun, treba napraviti novi segment, i treba azurirati sve podatke
		file.Close()
		if err := manager.rotateSegment(bm); err != nil {
			return nil, err, 0
		}
		return manager, nil, lastSeq
	}

	if currentOffset < 0 || currentOffset > int(header.BlockSize) {
		file.Close()
		return nil, fmt.Errorf("invalid WAL replay offset %d for block size %d", currentOffset, header.BlockSize), 0
	}
	if currentBlock < 0 || currentBlock > int(header.SegmentBlocks) {
		file.Close()
		return nil, fmt.Errorf("invalid WAL replay block index %d for segment blocks %d", currentBlock, header.SegmentBlocks), 0
	}

	remainingInBlock := int(header.BlockSize) - currentOffset

	manager.CurrentSegment = &WALSegment{
		File:             file,
		FilePath:         lastSegmentPath,
		Header:           header,
		CurrentBlock:     currentBlock,
		RemainingInBlock: remainingInBlock,
	}

	return manager, nil, lastSeq
}

func ReplayWAL(firstID int, lastID int, dirpath string, bm *block.BlockManager, applier RecordApplier) (int, int, error, uint64) {
	var completeData []byte
	lastSeq := uint64(0)
	for i := firstID; i <= lastID; i++ {
		segmentPath := fmt.Sprintf("%s/wal_%d.log", dirpath, i)
		segmentHeader, err := bm.ReadAt(segmentPath, 0, WALSegmentHeaderSize)
		offset := int(WALSegmentHeaderSize)
		if err != nil {
			return -1, -1, err, 0
		}

		header, err := DeserializeWALSegmentHeader(segmentHeader)
		if err != nil {
			return -1, -1, err, 0
		}
		blockSize := int(header.BlockSize)
		segmentBlocks := int(header.SegmentBlocks)
		if header.Magic != [4]byte{'W', 'A', 'L', 'S'} {
			return -1, -1, fmt.Errorf("invalid WAL segment magic in segment %d", i), 0
		}
		var crc32Val uint32
		var fragType FragmentType
		var dataLen uint32
		var fragmentHeader []byte
		j := 0
		blockData, err := bm.ReadBlock(segmentPath, uint64(j), blockSize)
		if err != nil {
			return -1, -1, err, 0
		}
		for j < segmentBlocks {
			if offset+WALFragmentHeaderSize > blockSize {
				fragmentHeader = blockData[offset:]
				crc32Val, fragType, dataLen, err = DeserializeWALFragmentHeader(fragmentHeader)
				if err != nil {
					return -1, -1, err, 0
				}
				if fragType == FragmentPadding {
					offset = 0
					j++
					if j != segmentBlocks {
						blockData, err = bm.ReadBlock(segmentPath, uint64(j), blockSize)
						if err != nil {
							return -1, -1, err, 0
						}
					}
					continue
				} else {
					return -1, -1, fmt.Errorf("unexpected fragment header at end of block in segment %d, block %d", i, j), 0
				}
			}
			fragmentHeader = blockData[offset : offset+WALFragmentHeaderSize]
			crc32Val, fragType, dataLen, err = DeserializeWALFragmentHeader(fragmentHeader)
			if err != nil {
				return -1, -1, err, 0
			}
			// Dosli smo do poslednjeg segmenta i kraj poslednjeg blokam u njemu
			if fragType == 0 && dataLen == 0 && crc32Val == 0 {
				if i != lastID {
					return -1, -1, fmt.Errorf("unexpected end marker before last segment at segment %d", i), 0
				}
				if len(completeData) > 0 {
					return -1, -1, fmt.Errorf("incomplete WAL record before end marker in segment %d, block %d", i, j), 0
				}
				return j, offset, nil, lastSeq
			}
			payloadEnd := offset + WALFragmentHeaderSize + int(dataLen)
			if payloadEnd > blockSize {
				return -1, -1, fmt.Errorf("fragment payload out of block bounds in segment %d, block %d", i, j), 0
			}
			data := blockData[offset+WALFragmentHeaderSize : payloadEnd]
			// Proveri CRC32
			if crc32Val != crc32.ChecksumIEEE(data) {
				return -1, -1, fmt.Errorf("fragment CRC32 mismatch in segment %d, block %d", i, j), 0
			}
			completeData = append(completeData, data...)
			if (fragType == FragmentFull) || (fragType == FragmentLast) {
				record, err := DeserializeWALRecord(completeData)
				lastSeq = record.Seq
				if err != nil {
					return -1, -1, err, 0
				}

				modelRecord := record.ToRecord()
				applier.ApplyRecord(modelRecord, true)

				completeData = nil
				offset += WALFragmentHeaderSize + int(dataLen)
				if offset == blockSize {
					offset = 0
					j++
					if j != segmentBlocks {
						blockData, err = bm.ReadBlock(segmentPath, uint64(j), blockSize)
						if err != nil {
							return -1, -1, err, 0
						}
					}
				}
			} else if (fragType == FragmentFirst) || (fragType == FragmentMiddle) {
				offset = 0
				j++
				if j != segmentBlocks {
					blockData, err = bm.ReadBlock(segmentPath, uint64(j), blockSize)
					if err != nil {
						return -1, -1, err, 0
					}
				}
			}
		}
	}
	if len(completeData) > 0 {
		return -1, -1, fmt.Errorf("incomplete WAL record at end of last segment"), 0
	}
	return 0, 0, nil, lastSeq
}

func parseSegmentID(filename string) int {
	if !strings.HasPrefix(filename, "wal_") || !strings.HasSuffix(filename, ".log") {
		return -1
	}
	base := strings.TrimPrefix(filename, "wal_")
	base = strings.TrimSuffix(base, ".log")
	id, err := strconv.Atoi(base)
	if err != nil {
		return -1
	}
	return id
}

func (m *WALManager) rotateSegment(bm *block.BlockManager) error {
	if m.CurrentSegment != nil {
		m.CurrentSegment.File.Close()
	}

	m.SegmentID++

	filePath := fmt.Sprintf("%s/wal_%d.log", m.DirPath, m.SegmentID)

	header := NewWalSegmentHeader(uint32(m.BlockSize), uint32(m.MaxSegmentBlocks))

	segment, err := NewWALSegment(filePath, header, bm)
	if err != nil {
		return err
	}

	m.CurrentSegment = segment
	return nil
}

func (m *WALManager) Write(seq uint64, expiresAt uint64, opType byte, key []byte, value []byte) error {
	if m.CurrentSegment == nil {
		return fmt.Errorf("current WAL segment is not initialized")
	}
	if m.bm == nil {
		return fmt.Errorf("block manager is not initialized")
	}

	walRecord := NewWALRecord(seq, expiresAt, opType, key, value)
	recordData := walRecord.Serialize()
	dataOffset := 0
	isFirstFragment := true

	for dataOffset < len(recordData) {
		if m.CurrentSegment.RemainingInBlock < WALFragmentHeaderSize+1 {
			paddingLen := m.CurrentSegment.RemainingInBlock
			if paddingLen > 0 {
				padding := make([]byte, paddingLen)
				if err := m.writeBytesToCurrentBlock(padding); err != nil {
					return err
				}
				m.CurrentSegment.RemainingInBlock = 0
			}
			if err := m.advanceBlockOrRotate(); err != nil {
				return err
			}
			continue
		}

		maxFragmentDataLen := m.CurrentSegment.RemainingInBlock - WALFragmentHeaderSize
		remainingRecordData := len(recordData) - dataOffset
		fragmentDataLen := remainingRecordData
		if fragmentDataLen > maxFragmentDataLen {
			fragmentDataLen = maxFragmentDataLen
		}

		fragmentData := recordData[dataOffset : dataOffset+fragmentDataLen]

		var fragType FragmentType
		if isFirstFragment && fragmentDataLen == remainingRecordData {
			fragType = FragmentFull
		} else if isFirstFragment {
			fragType = FragmentFirst
		} else if fragmentDataLen == remainingRecordData {
			fragType = FragmentLast
		} else {
			fragType = FragmentMiddle
		}

		fragment := NewWALFragment(fragType, uint32(fragmentDataLen), fragmentData)
		serializedFragment := fragment.Serialize()

		if err := m.writeBytesToCurrentBlock(serializedFragment); err != nil {
			return err
		}

		m.CurrentSegment.RemainingInBlock -= len(serializedFragment)
		dataOffset += fragmentDataLen
		isFirstFragment = false

		if m.CurrentSegment.RemainingInBlock == 0 {
			if err := m.advanceBlockOrRotate(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Append upisuje model.Record u WAL koristeci bitpacked OpType.
func (m *WALManager) Append(rec model.Record) error {
	var op byte
	if rec.Tombstone {
		op = OpDelete
	} else {
		op = OpPut
	}
	packed := PackOpType(op, rec.Kind, rec.Structure, rec.Op)
	return m.Write(rec.Seq, rec.ExpiresAt, packed, []byte(rec.Key), rec.Value)
}

func (m *WALManager) writeBytesToCurrentBlock(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if m.CurrentSegment == nil {
		return fmt.Errorf("current WAL segment is not initialized")
	}
	if m.bm == nil {
		return fmt.Errorf("block manager is not initialized")
	}

	blockSize := int(m.CurrentSegment.Header.BlockSize)
	if len(data) > m.CurrentSegment.RemainingInBlock {
		return fmt.Errorf("insufficient space in block: need %d bytes, remaining %d", len(data), m.CurrentSegment.RemainingInBlock)
	}

	blockNum := uint64(m.CurrentSegment.CurrentBlock)
	blockData, err := m.bm.ReadBlock(m.CurrentSegment.FilePath, blockNum, blockSize)
	if err != nil {
		return err
	}

	offset := blockSize - m.CurrentSegment.RemainingInBlock
	copy(blockData[offset:offset+len(data)], data)

	if err := m.bm.WriteBlock(m.CurrentSegment.FilePath, blockNum, blockData, blockSize); err != nil {
		return err
	}

	return nil
}

func (m *WALManager) advanceBlockOrRotate() error {
	if m.CurrentSegment == nil {
		return fmt.Errorf("current WAL segment is not initialized")
	}

	m.CurrentSegment.CurrentBlock++
	if m.CurrentSegment.CurrentBlock >= int(m.CurrentSegment.Header.SegmentBlocks) {
		if err := m.rotateSegment(m.bm); err != nil {
			return err
		}
		return nil
	}

	m.CurrentSegment.RemainingInBlock = int(m.CurrentSegment.Header.BlockSize)
	return nil
}
