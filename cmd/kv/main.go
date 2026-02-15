package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"kv-engine/internal/cli"
	"kv-engine/internal/config"
	"kv-engine/internal/engine"
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
  BF_ADD(key,value) / BF_REMOVE(key,value)
  BF_GET(key,value)
  CMS_ADD(key,value) / CMS_REMOVE(key,value)
  CMS_GET(key,value)
  HLL_ADD(key,value) / HLL_REMOVE(key,value)
  HLL_GET(key)
  // All above also support optional ttl: CMD(key,value,10s)
  GET(key)
  DELETE(key)
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

		case "BF_REMOVE":
			if err := runBinaryWriteCommand(args, "BF_REMOVE", eng.BFRemove); err != nil {
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

		case "CMS_REMOVE":
			if err := runBinaryWriteCommand(args, "CMS_REMOVE", eng.CMSRemove); err != nil {
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

		case "HLL_REMOVE":
			if err := runBinaryWriteCommand(args, "HLL_REMOVE", eng.HLLRemove); err != nil {
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
