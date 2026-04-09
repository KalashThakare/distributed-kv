package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/KalashThakare/distributed-kv/pkg/client"
)

// func main() {
// 	r := ring.New()
// 	r.AddNode("NodeA")
// 	r.AddNode("NodeB")
// 	r.AddNode("NodeC")

// 	fmt.Println("CLI connected successfully")
// 	scanner := bufio.NewScanner(os.Stdin)
// 	for {
// 		fmt.Println("> ")
// 		if !scanner.Scan() {
// 			break
// 		}
// 		parts := strings.Fields(scanner.Text())
// 		if len(parts) == 0 {
// 			continue
// 		}
// 		switch parts[0] {
// 		case "get":
// 			if len(parts) < 2 {
// 				fmt.Println("usage: get")
// 				continue
// 			}
// 			fmt.Printf("key %q -> %s\n", parts[1], r.GetNode(parts[1]))

// 		case "add":
// 			if len(parts) < 2 {
// 				fmt.Println("usage: add ")
// 				continue
// 			}
// 			r.AddNode(parts[1])
// 			fmt.Printf("added %s. nodes: %v\n", parts[1], r.Nodes())
// 		case "remove":
// 			if len(parts) < 2 {
// 				fmt.Println("usage: remove ")
// 				continue
// 			}
// 			r.RemoveNode(parts[1])
// 			fmt.Printf("removed %s. nodes: %v\n", parts[1], r.Nodes())
// 		case "nodes":
// 			fmt.Println(r.Nodes())
// 		case "bench":
// 			counts := map[string]int{}
// 			for _, name := range r.Nodes() {
// 				counts[name] = 0
// 			}
// 			for i := 0; i < 10000; i++ {
// 				key := fmt.Sprintf("key:%d", i)
// 				counts[r.GetNode(key)]++
// 			}
// 			fmt.Println("Distribution across 10,000 keys:")
// 			for node, count := range counts {
// 				bar := strings.Repeat("#", count/100)
// 				fmt.Printf(" %-8s %5d (%.1f%%) %s\n", node, count, float64(count)/100.0, bar)
// 			}
// 		case "exit", "quit":
// 			return
// 		default:
// 			fmt.Printf("unknown command: %q\n", parts[0])
// 		}
// 	}
// }

func main() {
	addr := "localhost:7001"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	client, err := client.New(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to %s: %v\n", addr, err)
		os.Exit(1)
	}

	defer client.Close()

	// check if server is alive

	name, keys, err := client.Health()

	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connected to %s (%s) — %d keys\n", addr, name, keys)
	fmt.Println("Commands: get | put | del | health | exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(">")

		if !scanner.Scan() {
			break
		}

		parts := strings.Fields(scanner.Text())
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "get":
			if len(parts) > 2 {
				fmt.Println("usage: get ")
				continue
			}

			val, found, err := client.Get(parts[1])
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else if !found {
				fmt.Printf("%q not found\n", parts[1])
			} else {
				fmt.Printf("%q = %q\n", parts[1], val)
			}

		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put ")
				continue
			}
			val := strings.Join(parts[2:], " ")

			if err := client.Put(parts[1], val); err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Printf("OK\n")
			}

		case "del", "delete":
			if len(parts) < 2 {
				fmt.Println("usage: del ")
				continue
			}

			if err := client.Delete(parts[1]); err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Printf("OK\n")
			}

		case "health":
			name, keys, err := client.Health()
			
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Printf("node=%s keys=%d\n", name, keys)
			}
		
		case "exit", "quit":
			return
		default:
			fmt.Printf("unknown command: %q\n", parts[0])
		}
	}
}
