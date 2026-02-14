package bloom

import (
	"bytes"
	"fmt"
)

func RunBloomFilterTests() {
	fmt.Println("=== Bloom Filter Basic Test ===")
	expected := 100
	fpRate := 0.01

	bf := NewBloomFilter(expected, fpRate)

	values := []string{"apple", "banana", "cherry"}

	// Add elements
	for _, v := range values {
		bf.Add([]byte(v))
	}

	// Check inserted elements
	allContained := true
	for _, v := range values {
		if !bf.MightContain([]byte(v)) {
			fmt.Println("❌ ERROR: Bloom filter should contain", v)
			allContained = false
		}
	}
	if allContained {
		fmt.Println("✅ All inserted elements present")
	}

	// Check false positives
	negatives := []string{"pear", "grape", "mango"}
	for _, v := range negatives {
		if bf.MightContain([]byte(v)) {
			fmt.Println("⚠️ Possible false positive detected for", v)
		}
	}

	fmt.Println("\n=== Bloom Filter Serialize/Deserialize Test ===")
	data, err := bf.Serialize()
	if err != nil {
		fmt.Println("❌ Serialize failed:", err)
		return
	}

	bf2, err := Deserialize(data)
	if err != nil {
		fmt.Println("❌ Deserialize failed:", err)
		return
	}

	// Check restored filter contains inserted values
	allContained = true
	for _, v := range values {
		if !bf2.MightContain([]byte(v)) {
			fmt.Println("❌ ERROR: Deserialized Bloom filter missing value", v)
			allContained = false
		}
	}
	if allContained {
		fmt.Println("✅ All elements correctly restored after deserialize")
	}

	// Verify bitset equality
	if !bytes.Equal(bf.bitset, bf2.bitset) {
		fmt.Println("❌ Bitset mismatch after deserialize")
	} else {
		fmt.Println("✅ Bitset matches after deserialize")
	}

	// Verify seed equality
	if bf.seed != bf2.seed {
		fmt.Println("❌ Seed mismatch after deserialize")
	} else {
		fmt.Println("✅ Seed matches after deserialize")
	}

	fmt.Println("=========================")
}
