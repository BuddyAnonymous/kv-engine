package sstable

import (
	"encoding/binary"
	"fmt"

	"kv-engine/internal/block"
	"kv-engine/internal/model"
)

const (
	crcBytes        = 4
	payloadLenBytes = 4
)

// ---------- Public API ----------

func (m *Manager) WriteMultiFile(path string, records []model.Record) error {
	if m.bm == nil {
		return fmt.Errorf("block manager is nil")
	}
	if m.blockSize <= 0 {
		return fmt.Errorf("invalid blockSize=%d", m.blockSize)
	}
	if len(records) == 0 {
		return nil
	}

	dataPath := path + ".data"
	indexPath := path + ".index"
	summPath := path + ".summary"
	// merklePath := path + ".merkle"
	// bloomFilterPath := path + ".bloom"

	// napiši .data i skupljaj blockFirstKey za svaki data blok
	dataFirstKeys, err := m.writeDataFile(dataPath, records)
	if err != nil {
		return err
	}

	// napiši .index: 1 entry po data bloku (blockFirstKey -> dataBlockNum)
	// i pritom skupljaj info o tome gde se nalazi svaki index entry (koji index block)
	indexEntryLoc, err := m.writeIndexFile(indexPath, dataFirstKeys)
	if err != nil {
		return err
	}

	// napiši .summary: stride, min/max key, i (svaki stride-ti index entry key -> indexBlockNum)
	if err := m.writeSummaryFile(summPath, indexEntryLoc, dataFirstKeys, records[len(records)-1].Key); err != nil {
		return err
	}

	return nil
}

func (m *Manager) WriteSingleFile(path string, records []model.Record) error {

	return nil
}

// ---------- Block writer helper ----------

type blockWriter struct {
	bm        *block.BlockManager
	path      string
	blockSize int

	curBlock   []byte
	curBlockNo uint64
	pos        int

	// callback kad se novi blok "započne" (da resetuješ npr. prevKey)
	onNewBlock func(blockNo uint64, isFirstBlock bool) error
}

func (w *blockWriter) SetOnNewBlock(cb func(blockNo uint64, isFirstBlock bool) error) {
	w.onNewBlock = cb
}

func newBlockWriter(bm *block.BlockManager, path string, blockSize int, onNewBlock func(blockNo uint64, isFirstBlock bool) error) *blockWriter {
	return &blockWriter{
		bm:         bm,
		path:       path,
		blockSize:  blockSize,
		curBlock:   make([]byte, blockSize),
		curBlockNo: 0,
		pos:        payloadLenBytes, // rezerviši početak bloka za payload size
		onNewBlock: onNewBlock,
	}
}

// ---------- DATA writing ----------

func (m *Manager) writeDataFile(dataPath string, recs []model.Record) ([]string, error) {

	//Prvi ključevi svakog data bloka, koji će ići u INDEX fajl kao blockFirstKey.
	var firstKeys []string
	ensureBlockSlot := func(blockNo uint64) {
		for len(firstKeys) <= int(blockNo) {
			firstKeys = append(firstKeys, "")
		}
	}

	prevKey := ""

	bw := newBlockWriter(m.bm, dataPath, m.blockSize, nil)

	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		prevKey = ""
		ensureBlockSlot(blockNo)
		if isFirst {
			return bw.writeBytes(m.encodeHeader(m.dataMagic))
		}
		return nil
	})

	if bw.onNewBlock != nil {
		if err := bw.onNewBlock(0, true); err != nil {
			return nil, err
		}
	}

	for _, r := range recs {
		// enkoduj prema trenutnom prevKey
		recBytes, newPrevKey, err := m.encodeDataRecord(prevKey, r)
		if err != nil {
			return nil, err
		}

		if bw.pos == payloadLenBytes {
			// prvi record u novom bloku: full key
			recBytes, newPrevKey, err = m.encodeDataRecord("", r)
			if err != nil {
				return nil, err
			}
		} else if prevKey == "" {
			recBytes, newPrevKey, err = m.encodeDataRecord("", r)
			if err != nil {
				return nil, err
			}
		}
		if bw.pos+len(recBytes) > bw.payloadAreaSize() {
			// FIRST: upiši koliko može u ovaj blok

			dataHeaderBytes, err := encodeDataHeader(prevKey, r)

			if err != nil {
				return nil, err
			}
			// Ukoliko header ne staje u ostatak bloka, flushuj trenutni blok i otvori novi
			if len(dataHeaderBytes) > bw.payloadAreaSize()-bw.pos {
				if err := bw.flushCurBlock(); err != nil {
					return nil, err
				}

				dataHeaderBytes, err = encodeDataHeader("", r)
				if err != nil {
					return nil, err
				}
				bw.curBlockNo++
				bw.curBlock = make([]byte, bw.blockSize)
				bw.pos = payloadLenBytes
				if bw.onNewBlock != nil {
					if err := bw.onNewBlock(bw.curBlockNo, false); err != nil {
						return nil, err
					}
				}

				// Record sada pocinje u novom bloku, mora biti full-key enkodovan.
				recBytes, newPrevKey, err = m.encodeDataRecord("", r)
				if err != nil {
					return nil, err
				}
			}

			ensureBlockSlot(bw.curBlockNo)
			if firstKeys[bw.curBlockNo] == "" {
				firstKeys[bw.curBlockNo] = r.Key
			}

			firstChunkLen := bw.payloadAreaSize() - bw.pos

			if firstChunkLen > len(recBytes) {
				firstChunkLen = len(recBytes)
			} else {
				recBytes[0] = (recBytes[0] &^ 0b11) | 0b10 // postavi FIRST bit u RecFlags (ako je fragmentiran, tj. ako ne staje ceo record u blok)
			}

			if err := bw.writeBytes(recBytes[:firstChunkLen]); err != nil {
				return nil, err
			}

			rest := recBytes[firstChunkLen:]
			if len(rest) > 0 {
				// zatvori trenutni blok
				if err := bw.flushCurBlock(); err != nil {
					return nil, err
				}
			}

			// piši ostatak kroz nove blokove
			for len(rest) > 0 {
				// pripremi novi blok
				bw.curBlockNo++
				bw.curBlock = make([]byte, bw.blockSize)
				bw.pos = payloadLenBytes

				if bw.onNewBlock != nil {
					if err := bw.onNewBlock(bw.curBlockNo, false); err != nil {
						return nil, err
					}
				}

				space := bw.payloadAreaSize() - bw.pos

				var tmp [10]byte
				ulen := binary.PutUvarint(tmp[:], uint64(len(rest))) // koliko bajtova zauzima varint za len(rest)

				if 1+ulen+len(rest) <= space {
					// LAST (poslednji fragment)
					// recFlags: zadnja 2 bita = LAST (01)
					flags := byte(0b01)

					if err := bw.writeBytes([]byte{flags}); err != nil {
						return nil, err
					}
					if err := bw.writeBytes(tmp[:ulen]); err != nil {
						return nil, err
					}
					if err := bw.writeBytes(rest); err != nil {
						return nil, err
					}

					rest = nil
					break
				}

				if space < 3 {
					if err := bw.flushCurBlock(); err != nil {
						return nil, err
					}
					continue
				}

				maxChunk := space - 1 // ostavi 1B za flags
				if maxChunk > len(rest) {
					maxChunk = len(rest)
				}

				for maxChunk > 0 {
					ulen := binary.PutUvarint(tmp[:], uint64(maxChunk))
					if 1+ulen+maxChunk <= space {
						// recFlags: zadnja 2 bita = MIDDLE (11)
						flags := byte(0b11)

						if err := bw.writeBytes([]byte{flags}); err != nil {
							return nil, err
						}
						if err := bw.writeBytes(tmp[:ulen]); err != nil {
							return nil, err
						}
						if err := bw.writeBytes(rest[:maxChunk]); err != nil {
							return nil, err
						}

						rest = rest[maxChunk:]
						break
					}
					maxChunk--
				}

				if maxChunk == 0 {
					return nil, fmt.Errorf("not enough space in block for MIDDLE fragment header")
				}

				if err := bw.flushCurBlock(); err != nil {
					return nil, err
				}
			}
			// fragmentacija je završila upis za ovaj record -> preskoči normalan writeBytes(recBytes)
			prevKey = newPrevKey
			continue
		}

		ensureBlockSlot(bw.curBlockNo)
		if firstKeys[bw.curBlockNo] == "" {
			firstKeys[bw.curBlockNo] = r.Key
		}

		if err := bw.writeBytes(recBytes); err != nil {
			return nil, err
		}
		prevKey = newPrevKey
	}

	if err := bw.close(); err != nil {
		return nil, err
	}
	return firstKeys, nil
}

// ---------- INDEX writing ----------

// indexEntryLoc: za svaki index entry pamti u kom INDEX bloku se nalazi.
type indexEntryLoc struct {
	indexEntryNo uint64
	indexBlockNo uint64
}

func (m *Manager) writeIndexFile(indexPath string, dataFirstKeys []string) ([]indexEntryLoc, error) {
	var locs []indexEntryLoc
	prevKey := ""

	bw := newBlockWriter(m.bm, indexPath, m.blockSize, nil)

	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		prevKey = ""
		if isFirst {
			return bw.writeBytes(m.encodeHeader(m.indexMagic))
		}
		return nil
	})

	// start block 0
	if bw.onNewBlock != nil {
		if err := bw.onNewBlock(0, true); err != nil {
			return nil, err
		}
	}

	for i, fk := range dataFirstKeys {
		if fk == "" {
			// ovaj data blok nema record, preskoči index entry (ne treba indexirati prazne blokove)
			continue
		}
		// encode prema trenutnom prevKey
		entryBytes, newPrevKey := encodeIndexEntry(prevKey, fk, uint64(i))

		// ako ne staje -> ensure će flush+novi blok (prevKey resetuje u onNewBlock)
		if err := bw.ensure(len(entryBytes)); err != nil {
			return nil, err
		}

		// ako smo prešli u novi blok, mora full key (prevKey="")
		if bw.pos == payloadLenBytes || bw.pos == 0 {
			entryBytes, newPrevKey = encodeIndexEntry("", fk, uint64(i))
		}

		// loc: u kom INDEX bloku je upisan ovaj index entry
		locs = append(locs, indexEntryLoc{
			indexEntryNo: uint64(i),
			indexBlockNo: bw.curBlockNo,
		})

		if err := bw.writeBytes(entryBytes); err != nil {
			return nil, err
		}
		prevKey = newPrevKey
	}

	if err := bw.close(); err != nil {
		return nil, err
	}
	return locs, nil
}

// ---------- SUMMARY writing ----------

func (m *Manager) writeSummaryFile(summaryPath string, locs []indexEntryLoc, dataFirstKeys []string, maxKey string) error {

	minKey := ""
	for _, k := range dataFirstKeys {
		if k != "" {
			minKey = k
			break
		}
	}

	prevKey := ""

	bw := newBlockWriter(m.bm, summaryPath, m.blockSize, nil)

	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		prevKey = ""
		if !isFirst {
			return nil
		}

		// header
		if err := bw.writeBytes(m.encodeHeader(m.summMagic)); err != nil {
			return err
		}
		// meta: STRIDE, MINKEYLEN+MINKEY, MAXKEYLEN+MAXKEY
		if err := bw.writeBytes(uvarintBytes(m.summaryStride)); err != nil {
			return err
		}
		if err := bw.writeBytes(uvarintBytes(uint64(len(minKey)))); err != nil {
			return err
		}
		if err := bw.writeBytes([]byte(minKey)); err != nil {
			return err
		}
		if err := bw.writeBytes(uvarintBytes(uint64(len(maxKey)))); err != nil {
			return err
		}
		if err := bw.writeBytes([]byte(maxKey)); err != nil {
			return err
		}
		return nil
	})

	if bw.onNewBlock != nil {
		if err := bw.onNewBlock(0, true); err != nil {
			return err
		}
	}

	stride := int(m.summaryStride)
	if stride <= 0 {
		return fmt.Errorf("invalid summaryStride=%d", m.summaryStride)
	}

	for i := 0; i < len(locs); i += stride {
		loc := locs[i]
		if loc.indexEntryNo >= uint64(len(dataFirstKeys)) {
			return fmt.Errorf("summary/index mismatch: indexEntryNo=%d out of range", loc.indexEntryNo)
		}

		key := dataFirstKeys[loc.indexEntryNo]
		if key == "" {
			continue
		}
		indexBlockNo := loc.indexBlockNo // blockNumber u .index

		entryBytes, newPrevKey := encodeSummaryEntry(prevKey, key, indexBlockNo)

		// ako ne staje, ensure otvara novi blok -> prevKey se resetuje
		if err := bw.ensure(len(entryBytes)); err != nil {
			return err
		}
		// ako smo u novom bloku, mora full key (prevKey="")
		if bw.pos == payloadLenBytes {

			entryBytes, newPrevKey = encodeSummaryEntry("", key, indexBlockNo)
		}

		if err := bw.writeBytes(entryBytes); err != nil {
			return err
		}
		prevKey = newPrevKey
	}

	return bw.close()
}
