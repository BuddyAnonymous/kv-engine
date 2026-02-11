package block

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var ErrInvalidBlockSize = errors.New("invalid block size")

// BlockKey identifikuje blok po putanji i broju bloka u tom fajlu
type BlockKey struct {
	Path     string
	BlockNum uint64
}

// BlockManager je klasa koja sluzi za citanje i pisanje blokova sa diska, sa cache-om
type BlockManager struct {
	cache *BlockCache
}

// Konstruktor za BlockManager
func NewBlockManager(cacheSize int) *BlockManager {
	return &BlockManager{
		cache: NewBlockCache(cacheSize),
	}
}

// ReadAt sluzi za citanje n bajtova sa diska u odnosu na offset, koristi se za citanje
// metapodataka(headera) bez upisivanja u cache
func (bm *BlockManager) ReadAt(path string, offset int64, size uint) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("file truncated: need %d bytes at offset %d, got %d", size, offset, n)
		}
		return nil, err
	}

	if uint(n) != size {
		return nil, fmt.Errorf("expected to read %d bytes, got %d", size, n)
	}

	return data, nil
}

// ReadBlock čita blok sa diska ili iz cache-a
func (bm *BlockManager) ReadBlock(path string, blockNum uint64, blockSize int) ([]byte, error) {
	key := BlockKey{Path: path, BlockNum: blockNum}

	// 1. Provera cache-a
	if data, ok := bm.cache.Get(key); ok {
		return data, nil
	}

	// 2. Čitanje sa diska
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := int64(blockNum) * int64(blockSize)
	data := make([]byte, blockSize)
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Ako je pročitan manji broj bajtova, popuni ostatak nulama
	if n < blockSize {
		for i := n; i < blockSize; i++ {
			data[i] = 0
		}
	}

	// 3. Upis u cache
	bm.cache.Put(key, data)

	return data, nil
}

// WriteBlock piše blok na disk i ažurira cache
func (bm *BlockManager) WriteBlock(path string, blockNum uint64, data []byte, blockSize int) error {
	if len(data) != blockSize {
		return ErrInvalidBlockSize
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(blockNum) * int64(blockSize)
	_, err = file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	// file.Sync()

	// 3. Upis u cache
	key := BlockKey{Path: path, BlockNum: blockNum}
	bm.cache.Put(key, data)

	return nil
}

// Upis bloka na kraj fajla, vraca broj bloka koji je upisan
func (bm *BlockManager) AppendBlock(path string, data []byte, blockSize int) (uint64, error) {
	if len(data) != blockSize {
		return 0, ErrInvalidBlockSize
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	size := info.Size()

	if size%int64(blockSize) != 0 {
		return 0, fmt.Errorf("file size %d is not aligned to block size %d", size, blockSize)
	}

	blockNum := uint64(size / int64(blockSize))
	offset := size

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return 0, err
	}

	// Upis u cache
	key := BlockKey{Path: path, BlockNum: blockNum}
	bm.cache.Put(key, data)

	return blockNum, nil
}
