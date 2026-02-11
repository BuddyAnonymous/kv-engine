package main

import (
	"fmt"
	"log"

	"kv-engine/internal/block"
)

func main() {
	blockSize := 8  // veličina jednog bloka u bajtovima
	cacheSize := 32 // veličina cache-a u bajtovima

	bm := block.NewBlockManager(cacheSize)

	filePath := "testfile.dat"

	// --- Pisanje blokova ---
	fmt.Println("Pisanje blokova...")
	for i := uint64(0); i < 5; i++ {
		data := make([]byte, blockSize)
		for j := 0; j < blockSize; j++ {
			data[j] = byte(i*10 + uint64(j))
		}
		if err := bm.WriteBlock(filePath, i, data, blockSize); err != nil {
			log.Fatal("WriteBlock error:", err)
		}
	}

	// --- Čitanje blokova (test cache) ---
	fmt.Println("Čitanje blokova...")
	for i := uint64(0); i < 5; i++ {
		data, err := bm.ReadBlock(filePath, i, blockSize)
		if err != nil {
			log.Fatal("ReadBlock error:", err)
		}
		fmt.Printf("Blok %d: %v\n", i, data)
	}

	data, err := bm.ReadBlock(filePath, 1, blockSize)
	fmt.Printf("%d", data)
	// --- Čitanje At offset (ReadAt) ---
	fmt.Println("Čitanje sa offset-a (ReadAt)...")
	offset := int64(8) // počni od drugog bloka
	readData, err := bm.ReadAt(filePath, offset, uint(blockSize))
	if err != nil {
		log.Fatal("ReadAt error:", err)
	}
	fmt.Printf("Podaci sa offset %d: %v\n", offset, readData)
}
