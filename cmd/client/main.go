package client

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/KalashThakare/distributed-kv/pkg/ring"
)

func main() {
	r := ring.New()
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	r.AddNode("NodeC")

	fmt.Println("Distributed KV")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("> ")
		if !scanner.Scan() {
			break
		}
		parts := strings.Fields(scanner.Text())
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get")
				continue
			}
			fmt.Printf("key %q -> %s\n", parts[1], r.GetNode(parts[1]))

		case "add":
			if len(parts) < 2 {
				fmt.Println("usage: add ")
				continue
			}
			r.AddNode(parts[1])
			fmt.Printf("added %s. nodes: %v\n", parts[1], r.Nodes())
		case "remove":
			if len(parts) < 2 {
				fmt.Println("usage: remove ")
				continue
			}
			r.RemoveNode(parts[1])
			fmt.Printf("removed %s. nodes: %v\n", parts[1], r.Nodes())
		case "nodes":
			fmt.Println(r.Nodes())
		case "bench":
			counts := map[string]int{}
			for _, name := range r.Nodes() {
				counts[name] = 0
			}
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("key:%d", i)
				counts[r.GetNode(key)]++
			}
			fmt.Println("Distribution across 10,000 keys:")
			for node, count := range counts {
				bar := strings.Repeat("#", count/100)
				fmt.Printf(" %-8s %5d (%.1f%%) %s\n", node, count, float64(count)/100.0, bar)
			}
		case "exit", "quit":
			return
		default:
			fmt.Printf("unknown command: %q\n", parts[0])
		}
	}
}
