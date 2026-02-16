package lsm

type LSMConfig struct {
	MaxLevels int    // koliko nivoa LSM stablo ima (min 2)
	Algorithm string // "size_tiered" ili "leveled"

	// Size-Tiered parametri
	SizeTieredMinSSTables int // minimalni broj SSTable-ova na nivou za pokretanje kompakcije

	// Leveled parametri
	LeveledL0Threshold int // broj SSTable-ova na L0 koji pokrece kompakciju
	LeveledBaseSizeMB  int // ciljna velicina L1 u MB
	LeveledMultiplier  int // faktor rasta izmedju nivoa (L_n+1 = multiplier * L_n)
}
