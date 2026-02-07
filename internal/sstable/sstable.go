package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"kv-engine/internal/model"
)

type Manager struct {
	dir              string
	multiFileSSTable bool
}

func New(dir string, multiFileSSTable bool) *Manager {
	return &Manager{dir: dir, multiFileSSTable: multiFileSSTable}
}

// [keyLen uvarint][valLen uvarint][tomb u8][seq uvarint][key][val]

func encodeRecord(r model.Record) []byte {
	key := []byte(r.Key)
	val := r.Value
	if r.Tombstone {
		val = nil
	}

	// varint maksimalno 10 bajtova za u64
	tmp := make([]byte, 10)

	buf := make([]byte, 0, 10+10+1+10+len(key)+len(val))

	// keyLen
	n := binary.PutUvarint(tmp, uint64(len(key)))
	buf = append(buf, tmp[:n]...)

	// valLen
	n = binary.PutUvarint(tmp, uint64(len(val)))
	buf = append(buf, tmp[:n]...)

	// tomb
	if r.Tombstone {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	// seq
	n = binary.PutUvarint(tmp, r.Seq)
	buf = append(buf, tmp[:n]...)

	// key + val
	buf = append(buf, key...)
	buf = append(buf, val...)

	return buf
}

func (m *Manager) Flush(records []model.Record) error {
	if m.multiFileSSTable {
		err := m.Flush_Data(records)
		err = m.Flush_Index(records)
		err = m.Flush_Summary(records)
		err = m.Flush_Filter(records)
		err = m.Flush_Merkle(records)
		if err != nil {
			return err
		}
		return nil
	} else {
		err := m.Flush_SingleFile(records)
		return err
	}
}

func (m *Manager) Flush_SingleFile(records []model.Record) error {
	return nil
}

func (m *Manager) Flush_Data(records []model.Record) error {
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

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	for _, r := range records {
		b := encodeRecord(r)
		if _, err := f.Write(b); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

func (m *Manager) Flush_Index(records []model.Record) error {
	return nil
}

func (m *Manager) Flush_Summary(records []model.Record) error {
	return nil
}

func (m *Manager) Flush_Filter(records []model.Record) error {
	return nil
}

func (m *Manager) Flush_Merkle(records []model.Record) error {
	return nil
}

func (m *Manager) Get(key string) model.GetResult {
	files, _ := filepath.Glob(filepath.Join(m.dir, "*.data"))
	sort.Strings(files)
	// najnoviji fajlovi prvo
	for i := len(files) - 1; i >= 0; i-- {
		if res, ok := scanFile(files[i], key); ok {
			return res
		}
	}

	return model.GetResult{Found: false}
}

// [keyLen uvarint][valLen uvarint][tomb u8][seq uvarint][key][val]

func scanFile(path string, key string) (model.GetResult, bool) {
	f, err := os.Open(path)
	if err != nil {
		return model.GetResult{}, false
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				return model.GetResult{}, false
			}
			return model.GetResult{}, false
		}

		valLen, err := binary.ReadUvarint(r)
		if err != nil {
			return model.GetResult{}, false
		}

		tombByte, err := r.ReadByte()
		if err != nil {
			return model.GetResult{}, false
		}
		tomb := tombByte == 1

		seq, err := binary.ReadUvarint(r)
		if err != nil {
			return model.GetResult{}, false
		}

		kb := make([]byte, keyLen)
		if _, err := io.ReadFull(r, kb); err != nil {
			return model.GetResult{}, false
		}

		vb := make([]byte, valLen)
		if _, err := io.ReadFull(r, vb); err != nil {
			return model.GetResult{}, false
		}

		if string(kb) == key {
			return model.GetResult{
				Key:       string(kb),
				Value:     vb,
				Found:     true,
				Tombstone: tomb,
				Seq:       seq,
			}, true
		}
	}
}
