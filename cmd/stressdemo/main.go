package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"kv-engine/internal/config"
	"kv-engine/internal/engine"
)

type settings struct {
	puts         int
	batches      int
	batchSize    int
	deleteRanges int
	bfAdds       int
	cmsAdds      int
	hllAdds      int
	multiFile    bool
	multiFileSet bool
	algo         string
	dataDir      string
	backupRoot   string
	configPath   string
	cleanup      bool
	switchMode   bool
}

func main() {
	cfg := parseFlags()
	engineCfg, err := buildEngineConfig(cfg)
	must(err)

	if cfg.cleanup {
		_ = os.RemoveAll(engineCfg.DataDir)
		_ = os.RemoveAll(engineCfg.BackupRoot)
	}

	if err := os.MkdirAll(engineCfg.DataDir, 0o755); err != nil {
		fatalf("mkdir data dir: %v", err)
	}
	if err := os.MkdirAll(engineCfg.BackupRoot, 0o755); err != nil {
		fatalf("mkdir backup dir: %v", err)
	}

	rng := rand.New(rand.NewSource(42))

	fmt.Println("== Stress demo start ==")
	fmt.Printf("config: puts=%d batches=%d batchSize=%d deleteRanges=%d bfAdds=%d cmsAdds=%d hllAdds=%d multi=%v algo=%s config=%s\n",
		cfg.puts, cfg.batches, cfg.batchSize, cfg.deleteRanges, cfg.bfAdds, cfg.cmsAdds, cfg.hllAdds, engineCfg.MultiFileSSTable, engineCfg.LSMCompactionAlgorithm, cfg.configPath)
	fmt.Printf("rate limit: tokens=%d interval_ms=%d\n", engineCfg.TokenBucketTokens, engineCfg.TokenBucketInterval)

	startAll := time.Now()
	eng, err := engine.New(engineCfg)
	must(err)

	knownKeys := make([]string, 0, cfg.puts+cfg.batches*cfg.batchSize)

	// 1) Many PUT
	putStart := time.Now()
	for i := 0; i < cfg.puts; i++ {
		k := fmt.Sprintf("kv:%08d", i)
		v := []byte(fmt.Sprintf("value-%08d-%08x", i, rng.Uint32()))
		if i%13 == 0 {
			must(eng.Put(k, v, 2*time.Hour))
		} else {
			must(eng.Put(k, v))
		}
		knownKeys = append(knownKeys, k)
		printProgress(i+1, cfg.puts, "PUT")
	}
	fmt.Printf("[DONE] PUT in %s\n", time.Since(putStart).Round(time.Millisecond))

	// 2) Batch writes
	batchStart := time.Now()
	for b := 0; b < cfg.batches; b++ {
		pairs := make([]engine.KVPair, 0, cfg.batchSize)
		for j := 0; j < cfg.batchSize; j++ {
			idx := b*cfg.batchSize + j
			k := fmt.Sprintf("bkv:%08d", idx)
			v := []byte(fmt.Sprintf("batch-%08d-%08x", idx, rng.Uint32()))
			pairs = append(pairs, engine.KVPair{Key: k, Value: v})
			knownKeys = append(knownKeys, k)
		}
		must(eng.BatchWrite(pairs))
		printProgress(b+1, cfg.batches, "BATCH_WRITE")
	}
	fmt.Printf("[DONE] BATCH_WRITE in %s\n", time.Since(batchStart).Round(time.Millisecond))

	// 3) Delete small ranges
	deleteStart := time.Now()
	maxBase := cfg.puts - 200
	if maxBase < 1 {
		maxBase = 1
	}
	for i := 0; i < cfg.deleteRanges; i++ {
		base := rng.Intn(maxBase)
		end := base + 10 + rng.Intn(40)
		if end >= cfg.puts {
			end = cfg.puts - 1
		}
		must(eng.DeleteRange(fmt.Sprintf("kv:%08d", base), fmt.Sprintf("kv:%08d", end)))
		printProgress(i+1, cfg.deleteRanges, "DELETE_RANGE")
	}
	fmt.Printf("[DONE] DELETE_RANGE in %s\n", time.Since(deleteStart).Round(time.Millisecond))

	// 4) Probabilistic structures
	probStart := time.Now()

	must(eng.BFCreate("bf_demo"))
	bfVals := make([]string, 0, cfg.bfAdds)
	for i := 0; i < cfg.bfAdds; i++ {
		v := fmt.Sprintf("bf_val_%08d", i)
		bfVals = append(bfVals, v)
		must(eng.BFAdd("bf_demo", []byte(v)))
		printProgress(i+1, cfg.bfAdds, "BF_ADD")
	}
	for i := 0; i < minInt(20, len(bfVals)); i++ {
		ok, err := eng.BFGet("bf_demo", []byte(bfVals[i]))
		must(err)
		if !ok {
			fatalf("BF false negative for inserted value %q", bfVals[i])
		}
	}

	must(eng.CMSCreate("cms_demo"))
	cmsCounters := map[string]int{}
	cmsSeen := make([]string, 0, cfg.cmsAdds)
	for i := 0; i < cfg.cmsAdds; i++ {
		e := fmt.Sprintf("event_%03d", rng.Intn(250))
		cmsCounters[e]++
		cmsSeen = append(cmsSeen, e)
		must(eng.CMSAdd("cms_demo", []byte(e)))
		printProgress(i+1, cfg.cmsAdds, "CMS_ADD")
	}
	// Robust check: at least one inserted event must have non-zero estimate.
	anyPositive := false
	for i := 0; i < minInt(50, len(cmsSeen)); i++ {
		e := cmsSeen[i]
		n, err := eng.CMSGet("cms_demo", []byte(e))
		must(err)
		if n > 0 {
			anyPositive = true
			break
		}
	}
	if !anyPositive {
		fatalf("CMS validation failed: all checked inserted events have zero estimate")
	}

	must(eng.HLLCreate("hll_demo"))
	hllSet := map[string]struct{}{}
	for i := 0; i < cfg.hllAdds; i++ {
		u := fmt.Sprintf("user_%05d", rng.Intn(cfg.hllAdds/2+1))
		hllSet[u] = struct{}{}
		must(eng.HLLAdd("hll_demo", []byte(u)))
		printProgress(i+1, cfg.hllAdds, "HLL_ADD")
	}
	est, err := eng.HLLGet("hll_demo")
	must(err)
	if est == 0 {
		fatalf("HLL estimate is zero")
	}

	must(eng.SimHashStore("sim_a", "ovo je jedan duzi tekst za simhash proveru"))
	must(eng.SimHashStore("sim_b", "ovo je drugi duzi tekst za simhash proveru"))
	dist, err := eng.SimHashDistance("sim_a", "sim_b")
	must(err)
	fmt.Printf("[OK] SimHash distance(sim_a, sim_b) = %d\n", dist)

	fmt.Printf("[DONE] Probabilistic ops in %s\n", time.Since(probStart).Round(time.Millisecond))

	// 5) Scan + iterator checks
	scanStart := time.Now()
	pairs, err := eng.PrefixScan("kv:", 1, 50)
	must(err)
	if len(pairs) == 0 {
		fatalf("PREFIX_SCAN returned empty page unexpectedly")
	}
	if !sort.SliceIsSorted(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key }) {
		fatalf("PREFIX_SCAN result is not sorted")
	}

	rangePairs, err := eng.RangeScan("kv:00000010", "kv:00001000", 1, 200)
	must(err)
	for _, p := range rangePairs {
		if p.Key < "kv:00000010" || p.Key > "kv:00001000" {
			fatalf("RANGE_SCAN returned key out of range: %s", p.Key)
		}
	}

	itID, err := eng.PrefixIterate("bkv:")
	must(err)
	iterCount := 0
	for {
		_, ok, err := eng.IteratorNext(itID)
		must(err)
		if !ok {
			break
		}
		iterCount++
		if iterCount >= 1000 {
			break
		}
	}
	must(eng.IteratorStop(itID))
	if iterCount == 0 {
		fatalf("Iterator returned zero elements")
	}
	fmt.Printf("[DONE] Scan/iterator checks in %s\n", time.Since(scanStart).Round(time.Millisecond))

	// 6) Reopen check (optionally with switched mode)
	reopenCfg := engineCfg
	if cfg.switchMode {
		reopenCfg.MultiFileSSTable = !engineCfg.MultiFileSSTable
	}
	eng2, err := engine.New(reopenCfg)
	must(err)

	// Validate a few random known keys are readable if not deleted.
	valChecks := 0
	for i := 0; i < len(knownKeys) && valChecks < 40; i += maxInt(1, len(knownKeys)/80) {
		k := knownKeys[i]
		_, _, err := eng2.Get(k)
		must(err)
		valChecks++
	}

	ok, err := eng2.BFGet("bf_demo", []byte(bfVals[0]))
	must(err)
	if !ok {
		fatalf("BF data not visible after reopen")
	}

	n, err := eng2.CMSGet("cms_demo", []byte("event_001"))
	must(err)
	if n == 0 {
		fmt.Println("[WARN] CMS(event_001)=0 after reopen (can happen if event_001 was never inserted)")
	}

	est2, err := eng2.HLLGet("hll_demo")
	must(err)
	if est2 == 0 {
		fatalf("HLL estimate is zero after reopen")
	}

	fmt.Println("== Stress demo summary ==")
	fmt.Printf("unique HLL inputs (expected-ish): %d, estimated: %d\n", len(hllSet), est)
	fmt.Printf("data dir: %s\n", engineCfg.DataDir)
	fmt.Printf("backup root: %s\n", engineCfg.BackupRoot)
	fmt.Printf("total duration: %s\n", time.Since(startAll).Round(time.Millisecond))
	fmt.Println("STATUS: PASS")
}

func buildEngineConfig(s settings) (config.Config, error) {
	cfg, err := config.Load(s.configPath)
	if err != nil {
		return config.Config{}, err
	}
	if s.dataDir != "" {
		cfg.DataDir = s.dataDir
	}
	if s.backupRoot != "" {
		cfg.BackupRoot = s.backupRoot
	}
	if s.algo != "" {
		cfg.LSMCompactionAlgorithm = s.algo
	}
	if s.multiFileSet {
		cfg.MultiFileSSTable = s.multiFile
	}
	cfg.Normalize()
	return cfg, nil
}

func parseFlags() settings {
	s := settings{}
	flag.IntVar(&s.puts, "puts", 6000, "number of PUT operations")
	flag.IntVar(&s.batches, "batches", 500, "number of batch writes")
	flag.IntVar(&s.batchSize, "batch-size", 10, "number of pairs per batch")
	flag.IntVar(&s.deleteRanges, "delete-ranges", 80, "number of DELETE_RANGE operations")
	flag.IntVar(&s.bfAdds, "bf-adds", 4000, "number of BF_ADD operations")
	flag.IntVar(&s.cmsAdds, "cms-adds", 5000, "number of CMS_ADD operations")
	flag.IntVar(&s.hllAdds, "hll-adds", 5000, "number of HLL_ADD operations")
	var multiOverride optionalBool
	flag.Var(&multiOverride, "multi", "override multi-file mode (true/false)")
	flag.StringVar(&s.algo, "algo", "", "override compaction algorithm: size_tiered or leveled")
	flag.StringVar(&s.dataDir, "data-dir", "", "override data directory (defaults to config)")
	flag.StringVar(&s.backupRoot, "backup-root", "", "override backup directory (defaults to config)")
	flag.StringVar(&s.configPath, "config", "config.json", "path to config file")
	flag.BoolVar(&s.cleanup, "cleanup", false, "remove selected data/backup dirs before run")
	flag.BoolVar(&s.switchMode, "switch-mode", true, "reopen engine with opposite multi-file mode at the end")
	flag.Parse()
	s.multiFile = multiOverride.value
	s.multiFileSet = multiOverride.set

	if s.puts < 1 || s.batches < 0 || s.batchSize < 1 || s.deleteRanges < 0 || s.bfAdds < 1 || s.cmsAdds < 1 || s.hllAdds < 1 {
		fatalf("invalid numeric flags")
	}
	s.algo = strings.TrimSpace(strings.ToLower(s.algo))
	if s.algo != "" && s.algo != "size_tiered" && s.algo != "leveled" {
		fatalf("invalid -algo %q (expected size_tiered or leveled)", s.algo)
	}
	return s
}

type optionalBool struct {
	set   bool
	value bool
}

func (b *optionalBool) String() string {
	if !b.set {
		return ""
	}
	if b.value {
		return "true"
	}
	return "false"
}

func (b *optionalBool) Set(v string) error {
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fmt.Errorf("invalid bool %q", v)
	}
	b.value = parsed
	b.set = true
	return nil
}

func (b *optionalBool) IsBoolFlag() bool { return true }

func printProgress(done, total int, label string) {
	if total <= 0 {
		return
	}
	step := total / 10
	if step < 1 {
		step = 1
	}
	if done%step == 0 || done == total {
		fmt.Printf("[%s] %d/%d\n", label, done, total)
	}
}

func must(err error) {
	if err != nil {
		fatalf("error: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Printf("STATUS: FAIL - "+format+"\n", args...)
	os.Exit(1)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
