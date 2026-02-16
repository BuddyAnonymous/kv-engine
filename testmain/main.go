package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"kv-engine/internal/block"
	"kv-engine/internal/wal"
)

func makePayload(seed byte, length int) []byte {
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = seed + byte(i%17)
	}
	return data
}

func main() {
	walDir := filepath.Join("testmain", "wal_data")
	if err := os.RemoveAll(walDir); err != nil {
		log.Fatal("RemoveAll error:", err)
	}

	blockSize := 64
	segmentBlocks := 3
	cacheSize := 1 << 20

	bm := block.NewBlockManager(cacheSize)

	manager, err, _ := wal.NewWALManager(walDir, segmentBlocks, blockSize, bm)
	if err != nil {
		log.Fatal("NewWALManager error:", err)
	}

	fmt.Println("=== WAL WRITE TEST ===")
	fmt.Printf("Initial segment: id=%d currentBlock=%d remaining=%d\n", manager.SegmentID, manager.CurrentSegment.CurrentBlock, manager.CurrentSegment.RemainingInBlock)

	for i := 0; i < 18; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		valueLen := 15 + (i * 23 % 140)
		value := makePayload(byte(i+1), valueLen)

		if err := manager.Write(uint64(i+1), 0, wal.OpPut, key, value); err != nil {
			log.Fatalf("Write PUT error at i=%d: %v", i, err)
		}

		fmt.Printf("PUT  seq=%2d key=%s valueLen=%3d -> seg=%d block=%d rem=%d\n",
			i+1,
			key,
			len(value),
			manager.SegmentID,
			manager.CurrentSegment.CurrentBlock,
			manager.CurrentSegment.RemainingInBlock,
		)
	}

	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i*3))
		if err := manager.Write(uint64(100+i), 0, wal.OpDelete, key, nil); err != nil {
			log.Fatalf("Write DELETE error at i=%d: %v", i, err)
		}

		fmt.Printf("DEL  seq=%2d key=%s -> seg=%d block=%d rem=%d\n",
			100+i,
			key,
			manager.SegmentID,
			manager.CurrentSegment.CurrentBlock,
			manager.CurrentSegment.RemainingInBlock,
		)
	}

	fmt.Println("\n=== WAL RESTART + REPLAY TEST ===")
	recovered, err, _ := wal.NewWALManager(walDir, segmentBlocks, blockSize, bm)
	if err != nil {
		log.Fatal("NewWALManager(restart) error:", err)
	}

	fmt.Printf("Recovered manager: firstID=%d lastID=%d currentBlock=%d remaining=%d\n",
		recovered.FirstSegmentID,
		recovered.SegmentID,
		recovered.CurrentSegment.CurrentBlock,
		recovered.CurrentSegment.RemainingInBlock,
	)

	blockIdx, offset, err, _ := wal.ReplayWAL(recovered.FirstSegmentID, recovered.SegmentID, walDir, bm)
	if err != nil {
		log.Fatal("ReplayWAL error:", err)
	}
	fmt.Printf("Replay end position: block=%d offset=%d\n", blockIdx, offset)

	entries, err := os.ReadDir(walDir)
	if err != nil {
		log.Fatal("ReadDir walDir error:", err)
	}

	fmt.Println("\nSegment files:")
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			log.Fatal("Info error:", err)
		}
		fmt.Printf("- %s (%d bytes)\n", e.Name(), info.Size())
	}

	fmt.Println("\nWAL smoke test finished.")
}
