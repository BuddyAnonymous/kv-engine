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
			if len(args) != 2 && len(args) != 3 {
				fmt.Println(`usage:
  PUT(key,value)
  PUT(key,value,ttl)  // ttl like 10s, 5m, 2h`)
				continue
			}

			key := args[0]
			value := []byte(args[1])

			// no ttl
			if len(args) == 2 {
				if err := eng.Put(key, value); err != nil {
					fmt.Println("error:", err)
					continue
				}
				fmt.Println("OK")
				continue
			}

			// ttl provided
			dur, err := time.ParseDuration(args[2])
			if err != nil {
				fmt.Println("invalid TTL, use 10s, 5m, 2h")
				continue
			}

			if err := eng.Put(key, value, dur); err != nil {
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
