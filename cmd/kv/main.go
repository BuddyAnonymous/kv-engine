package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

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

	fmt.Println("KV engine ready. Commands: PUT k v | GET k | DELETE k | EXIT")

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

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("usage: PUT key value")
				continue
			}
			if err := eng.Put(parts[1], []byte(parts[2])); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "GET":
			if len(parts) < 2 {
				fmt.Println("usage: GET key")
				continue
			}
			val, ok, err := eng.Get(parts[1])
			if err != nil {
				fmt.Println("error:", err)
				continue
			}
			if !ok {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(val))
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("usage: DELETE key")
				continue
			}
			if err := eng.Delete(parts[1]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")

		case "EXIT", "QUIT":
			return

		default:
			fmt.Println("unknown command")
		}
	}
}
