package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"kv-engine/internal/model"
)

type Manager struct {
	dir string
}

func New(dir string) *Manager {
	return &Manager{dir: dir}
}

// format zapisa (minimalno):
// [keyLen u32][valLen u32][tomb u8][seq u64][key][val]
func encodeRecord(r model.Record) []byte {
	key := []byte(r.Key)
	val := r.Value
	if r.Tombstone {
		val = nil
	}

	buf := make([]byte, 4+4+1+8+len(key)+len(val))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
	if r.Tombstone {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	binary.LittleEndian.PutUint64(buf[9:17], r.Seq)
	copy(buf[17:17+len(key)], key)
	copy(buf[17+len(key):], val)
	return buf
}

func (m *Manager) Flush(records []model.Record) error {
	if err := os.MkdirAll(m.dir, 0755); err != nil {
		return err
	}

	name := fmt.Sprintf("sst_%d.data", time.Now().UnixNano())
	path := filepath.Join(m.dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, r := range records {
		b := encodeRecord(r)
		if _, err := f.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Get(key string) model.GetResult {
	files, _ := filepath.Glob(filepath.Join(m.dir, "*.data"))

	// najnoviji fajlovi prvo
	for i := len(files) - 1; i >= 0; i-- {
		if res, ok := scanFile(files[i], key); ok {
			return res
		}
	}

	return model.GetResult{Found: false}
}

// format koji si koristio u Flush():
// [keyLen u32][valLen u32][tomb u8][seq u64][key][val]
func scanFile(path string, key string) (model.GetResult, bool) {
	f, err := os.Open(path)
	if err != nil {
		return model.GetResult{}, false
	}
	defer f.Close()

	for {
		hdr := make([]byte, 4+4+1+8)
		_, err := f.Read(hdr)
		if err != nil {
			return model.GetResult{}, false // EOF
		}

		keyLen := binary.LittleEndian.Uint32(hdr[0:4])
		valLen := binary.LittleEndian.Uint32(hdr[4:8])
		tomb := hdr[8] == 1
		seq := binary.LittleEndian.Uint64(hdr[9:17])

		kb := make([]byte, keyLen)
		if _, err := f.Read(kb); err != nil {
			return model.GetResult{}, false
		}

		vb := make([]byte, valLen)
		if _, err := f.Read(vb); err != nil {
			return model.GetResult{}, false
		}

		if string(kb) == key {
			return model.GetResult{
				Value:     vb,
				Found:     true,
				Tombstone: tomb,
				Seq:       seq,
			}, true
		}
	}
}
