package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func (w *blockWriter) ensure(n int) error {
	if n > w.payloadCap() {
		return fmt.Errorf("record too large for single block payload: need=%d cap=%d", n, w.payloadCap())
	}
	if w.pos+n <= w.payloadCap() {
		return nil
	}
	if err := w.flushCurBlock(); err != nil {
		return err
	}
	w.curBlockNo++
	w.curBlock = make([]byte, w.blockSize)
	w.pos = 0
	if w.onNewBlock != nil {
		return w.onNewBlock(w.curBlockNo, false)
	}
	return nil
}

func (w *blockWriter) writeBytes(b []byte) error {
	if err := w.ensure(len(b)); err != nil {
		return err
	}
	copy(w.curBlock[w.pos:], b)
	w.pos += len(b)
	return nil
}

func (w *blockWriter) flushCurBlock() error {
	crc := crc32.ChecksumIEEE(w.curBlock[:w.payloadCap()])
	binary.LittleEndian.PutUint32(w.curBlock[w.payloadCap():], crc)
	return w.bm.WriteBlock(w.path, w.curBlockNo, w.curBlock, w.blockSize)
}

func (w *blockWriter) close() error {
	return w.flushCurBlock()
}

func uvarintBytes(x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return tmp[:n]
}

func sharedPrefixLen(a, b string) int {
	// a = prevKey, b = currentKey
	na, nb := len(a), len(b)
	n := na
	if nb < n {
		n = nb
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
