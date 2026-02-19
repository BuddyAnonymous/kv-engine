package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"kv-engine/internal/cli"
	"kv-engine/internal/config"
	"kv-engine/internal/engine"
	"kv-engine/internal/model"
)

func main() {

	cfg, err := config.Load("config.json")
	if err != nil {
		fmt.Println("config error:", err)
		os.Exit(1)
	}

	eng, err := engine.New(cfg)
	if err != nil {
		fmt.Println("engine init error:", err)
		os.Exit(1)
	}

	fmt.Print(`KV engine ready.
Formats:
  PUT(key,value)
  PUT(key,"value with spaces")
  PUT(key,value,10s)   // TTL optional: 10s / 5m / 2h
  BF_CREATE(key) / BF_DELETE(key)
  BF_ADD(key,value)
  BF_GET(key,value)
  CMS_CREATE(key) / CMS_DELETE(key)
  CMS_ADD(key,value)
  CMS_GET(key,value)
  HLL_CREATE(key) / HLL_DELETE(key)
  HLL_ADD(key,value)
  HLL_GET(key)
  SIMHASH_STORE(name,text)
  SIMHASH_GET(name)
  SIMHASH_DISTANCE(name1,name2)
  PREFIX_SCAN(prefix,pageNumber,pageSize)
  RANGE_SCAN(minKey,maxKey,pageNumber,pageSize)
  PREFIX_ITERATE(prefix)
  RANGE_ITERATE(minKey,maxKey)
  ITER_NEXT(iteratorId)
  ITER_STOP(iteratorId)
  // All above also support optional ttl: CMD(key,value,10s)
  GET(key)
  DELETE(key)
  MERKLE_VALIDATE(sstable_name)
  BACKUP
  EXIT
`)

	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		cmd, args, ok, errMsg := cli.ParseCall(line)
		if !ok {
			if errMsg != "" {
				fmt.Println("parse error:", errMsg)
			}
			continue
		}

		switch cmd {
		case "EXIT", "QUIT":
			return

		case "BACKUP":
			if err := eng.Backup(); err != nil {
				fmt.Println("backup error:", err)
			} else {
				fmt.Println("backup completed")
			}
		case "GET":
			if len(args) != 1 {
				fmt.Println("usage: GET(key)")
				continue
			}
			val, found, err := eng.Get(args[0])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if !found {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(val))
			}

		case "DELETE":
			if len(args) != 1 {
				fmt.Println("usage: DELETE(key)")
				continue
			}
			if err := eng.Delete(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "MERKLE_VALIDATE":
			if len(args) != 1 {
				fmt.Println("usage: MERKLE_VALIDATE(sstable_name)")
				continue
			}
			res, err := eng.ValidateMerkle(args[0])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if res.Valid {
				fmt.Printf("MERKLE OK root=%s leaves=%d\n", res.ActualRootHex, res.ActualLeafCount)
				continue
			}
			fmt.Printf("MERKLE MISMATCH expectedRoot=%s actualRoot=%s expectedLeaves=%d actualLeaves=%d changedLeafIndices=%v\n",
				res.ExpectedRootHex, res.ActualRootHex, res.ExpectedLeafCount, res.ActualLeafCount, res.ChangedLeafIndices)

		case "PUT":
			if err := runBinaryWriteCommand(args, "PUT", eng.Put); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "BF_ADD":
			if err := runBinaryWriteCommand(args, "BF_ADD", eng.BFAdd); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "BF_CREATE":
			if len(args) != 1 {
				fmt.Println("usage: BF_CREATE(key)")
				continue
			}
			if err := eng.BFCreate(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "BF_DELETE":
			if len(args) != 1 {
				fmt.Println("usage: BF_DELETE(key)")
				continue
			}
			if err := eng.BFDelete(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "CMS_ADD":
			if err := runBinaryWriteCommand(args, "CMS_ADD", eng.CMSAdd); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "CMS_CREATE":
			if len(args) != 1 {
				fmt.Println("usage: CMS_CREATE(key)")
				continue
			}
			if err := eng.CMSCreate(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "CMS_DELETE":
			if len(args) != 1 {
				fmt.Println("usage: CMS_DELETE(key)")
				continue
			}
			if err := eng.CMSDelete(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "HLL_ADD":
			if err := runBinaryWriteCommand(args, "HLL_ADD", eng.HLLAdd); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "HLL_CREATE":
			if len(args) != 1 {
				fmt.Println("usage: HLL_CREATE(key)")
				continue
			}
			if err := eng.HLLCreate(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "HLL_DELETE":
			if len(args) != 1 {
				fmt.Println("usage: HLL_DELETE(key)")
				continue
			}
			if err := eng.HLLDelete(args[0]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "BF_GET":
			if len(args) != 2 {
				fmt.Println("usage: BF_GET(key,value)")
				continue
			}
			ok, err := eng.BFGet(args[0], []byte(args[1]))
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(ok)

		case "CMS_GET":
			if len(args) != 2 {
				fmt.Println("usage: CMS_GET(key,value)")
				continue
			}
			n, err := eng.CMSGet(args[0], []byte(args[1]))
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(n)

		case "HLL_GET":
			if len(args) != 1 {
				fmt.Println("usage: HLL_GET(key)")
				continue
			}
			n, err := eng.HLLGet(args[0])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(n)

		case "SIMHASH_STORE":
			if len(args) != 2 {
				fmt.Println("usage: SIMHASH_STORE(name,text)")
				continue
			}
			if err := eng.SimHashStore(args[0], args[1]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "SIMHASH_GET":
			if len(args) != 1 {
				fmt.Println("usage: SIMHASH_GET(name)")
				continue
			}
			fp, found, err := eng.SimHashGet(args[0])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if !found {
				fmt.Println("(nil)")
				continue
			}
			fmt.Println(fp)

		case "SIMHASH_DISTANCE":
			if len(args) != 2 {
				fmt.Println("usage: SIMHASH_DISTANCE(name1,name2)")
				continue
			}
			d, err := eng.SimHashDistance(args[0], args[1])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(d)

		case "PREFIX_SCAN":
			if len(args) != 3 {
				fmt.Println("usage: PREFIX_SCAN(prefix,pageNumber,pageSize)")
				continue
			}
			pageNumber, pageSize, err := parsePageArgs(args[1], args[2])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			pairs, err := eng.PrefixScan(args[0], pageNumber, pageSize)
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			printPairs(pairs)

		case "RANGE_SCAN":
			if len(args) != 4 {
				fmt.Println("usage: RANGE_SCAN(minKey,maxKey,pageNumber,pageSize)")
				continue
			}
			pageNumber, pageSize, err := parsePageArgs(args[2], args[3])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			pairs, err := eng.RangeScan(args[0], args[1], pageNumber, pageSize)
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			printPairs(pairs)

		case "PREFIX_ITERATE":
			if len(args) != 1 {
				fmt.Println("usage: PREFIX_ITERATE(prefix)")
				continue
			}
			id, err := eng.PrefixIterate(args[0])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(id)

		case "RANGE_ITERATE":
			if len(args) != 2 {
				fmt.Println("usage: RANGE_ITERATE(minKey,maxKey)")
				continue
			}
			id, err := eng.RangeIterate(args[0], args[1])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println(id)

		case "ITER_NEXT":
			if len(args) != 1 {
				fmt.Println("usage: ITER_NEXT(iteratorId)")
				continue
			}
			id, err := parseUint64Arg(args[0], "iteratorId")
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			pair, ok, err := eng.IteratorNext(id)
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if !ok {
				fmt.Println("(end)")
				continue
			}
			fmt.Printf("%s=%s\n", pair.Key, string(pair.Value))

		case "ITER_STOP":
			if len(args) != 1 {
				fmt.Println("usage: ITER_STOP(iteratorId)")
				continue
			}
			id, err := parseUint64Arg(args[0], "iteratorId")
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if err := eng.IteratorStop(id); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		default:
			fmt.Println("unknown command")
		}
	}

	if err := sc.Err(); err != nil {
		fmt.Println("input error:", err)
	}
}

func runBinaryWriteCommand(args []string, name string, fn func(string, []byte, ...time.Duration) error) error {

	if len(args) != 2 && len(args) != 3 {
		return fmt.Errorf("usage:\n  %s(key,value)\n  %s(key,value,ttl)  // ttl like 10s, 5m, 2h", name, name)
	}

	key := args[0]
	value := []byte(args[1])
	if len(args) == 2 {
		return fn(key, value)
	}

	dur, err := time.ParseDuration(args[2])
	if err != nil {
		return fmt.Errorf("invalid TTL, use 10s, 5m, 2h")
	}
	return fn(key, value, dur)
}

func parsePageArgs(pageNumberArg, pageSizeArg string) (int, int, error) {

	pageNumber, err := strconv.Atoi(pageNumberArg)
	if err != nil || pageNumber < 1 {
		return 0, 0, fmt.Errorf("pageNumber must be integer >= 1")
	}

	pageSize, err := strconv.Atoi(pageSizeArg)
	if err != nil || pageSize < 1 {
		return 0, 0, fmt.Errorf("pageSize must be integer >= 1")
	}
	return pageNumber, pageSize, nil
}

func parseUint64Arg(raw string, name string) (uint64, error) {

	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be uint64", name)
	}
	return v, nil
}

func printPairs(pairs []model.KVPair) {

	if len(pairs) == 0 {
		fmt.Println("(empty)")
		return
	}
	for _, p := range pairs {
		fmt.Printf("%s=%s\n", p.Key, string(p.Value))
	}
}
