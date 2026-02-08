package bloom

import (
	"bytes"
	"fmt"
)

func RunBloomFilterTest() {
	fmt.Println("=== Bloom Filter Test ===")

	// parametri
	expected := 100
	fpRate := 0.01

	bf := NewBloomFilter(expected, fpRate)

	// test podaci
	values := []string{
		"apple",
		"banana",
		"cherry",
	}

	// add
	for _, v := range values {
		bf.Add([]byte(v))
	}

	// should be true
	for _, v := range values {
		if !bf.MightContain([]byte(v)) {
			fmt.Println("❌ ERROR: should contain:", v)
			return
		}
	}

	// should probably be false
	negatives := []string{
		"pear",
		"grape",
		"mango",
	}

	for _, v := range negatives {
		if bf.MightContain([]byte(v)) {
			fmt.Println("⚠️ false positive:", v)
		}
	}

	// serialize
	data, err := bf.Serialize()
	if err != nil {
		fmt.Println("❌ serialize failed:", err)
		return
	}

	// deserialize
	bf2, err := Deserialize(data)
	if err != nil {
		fmt.Println("❌ deserialize failed:", err)
		return
	}

	// verify after restore
	for _, v := range values {
		if !bf2.MightContain([]byte(v)) {
			fmt.Println("❌ ERROR after deserialize:", v)
			return
		}
	}

	// bitset equality
	if !bytes.Equal(bf.bitset, bf2.bitset) {
		fmt.Println("❌ bitset mismatch after deserialize")
		return
	}

	fmt.Println("✅ Bloom Filter works correctly!")
	fmt.Println("=========================")
}
