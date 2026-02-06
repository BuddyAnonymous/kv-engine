package block

import (
	"fmt"
	"os"
)

type Manager struct {
	BlockSize int64
}

func New(blockSize int) *Manager {
	return &Manager{BlockSize: int64(blockSize)}
}

func (m *Manager) ensureDir(path string) error {
	// napravi parent direktorijum ako treba
	// (za jednostavnost: ako je path "data/x", data mora postojati)
	return nil
}

func (m *Manager) ReadBlock(path string, blockNo int64) ([]byte, error) {
	if blockNo < 0 {
		return nil, fmt.Errorf("blockNo < 0")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, m.BlockSize)
	off := blockNo * m.BlockSize

	n, err := f.ReadAt(buf, off)
	// ako je fajl kraci, ReadAt vrati EOF, ali buf je i dalje ok (dopunjen nulama)
	if err != nil && n == 0 {
		// dozvoli EOF samo ako je delimično pročitano ili fajl kratak
		// ovde ćemo vratiti buf i err samo ako ništa nismo dobili
		return nil, err
	}
	return buf, nil
}

func (m *Manager) WriteBlock(path string, blockNo int64, data []byte) error {
	if blockNo < 0 {
		return fmt.Errorf("blockNo < 0")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, m.BlockSize)
	copy(buf, data)

	off := blockNo * m.BlockSize
	_, err = f.WriteAt(buf, off)
	return err
}
