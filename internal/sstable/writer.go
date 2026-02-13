package sstable

import (
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

	prevKey := ""

	bw := newBlockWriter(m.bm, dataPath, m.blockSize, nil)

	bw.SetOnNewBlock(func(blockNo uint64, isFirst bool) error {
		prevKey = ""
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

		// ako ne staje, pređi u novi blok pa re-encode sa prevKey=""
		if err := bw.ensure(len(recBytes)); err != nil {
			return nil, err
		}
		if bw.pos == 0 {
			// prvi record u novom bloku: full key
			recBytes, newPrevKey, err = m.encodeDataRecord("", r)
			if err != nil {
				return nil, err
			}
			firstKeys = append(firstKeys, r.Key)
		} else if prevKey == "" {

			firstKeys = append(firstKeys, r.Key)
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
		// encode prema trenutnom prevKey
		entryBytes, newPrevKey := encodeIndexEntry(prevKey, fk, uint64(i))

		// ako ne staje -> ensure će flush+novi blok (prevKey resetuje u onNewBlock)
		if err := bw.ensure(len(entryBytes)); err != nil {
			return nil, err
		}

		// ako smo prešli u novi blok, mora full key (prevKey="")
		if bw.pos == 0 {
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
	if len(dataFirstKeys) > 0 {
		minKey = dataFirstKeys[0]
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

	maxN := len(locs)
	if len(dataFirstKeys) < maxN {
		maxN = len(dataFirstKeys)
	}

	for i := uint64(0); i < uint64(maxN); i += m.summaryStride {
		key := dataFirstKeys[i]
		indexBlockNo := locs[i].indexBlockNo // blockNumber u .index

		entryBytes, newPrevKey := encodeSummaryEntry(prevKey, key, indexBlockNo)

		// ako ne staje, ensure otvara novi blok -> prevKey se resetuje
		if err := bw.ensure(len(entryBytes)); err != nil {
			return err
		}
		// ako smo u novom bloku, mora full key (prevKey="")
		if bw.pos == 0 {
			entryBytes, newPrevKey = encodeSummaryEntry("", key, indexBlockNo)
		}

		if err := bw.writeBytes(entryBytes); err != nil {
			return err
		}
		prevKey = newPrevKey
	}

	return bw.close()
}
