package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/KalashThakare/distributed-kv/pkg/cluster"
	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/server"
	"github.com/KalashThakare/distributed-kv/pkg/store"
)

func main() {
	// Flags
	port := flag.String("port", "8082", "gRPC listen port")
	name := flag.String("name", "NodeA", "node name (unique in cluster)")
	dataDir := flag.String("data-dir", "/temp", "directory for WAL and bbolt files")
	gossipPort := flag.Int("gossip-port", 7946, "gossip UDP port")
	peersFlag := flag.String("peers", "", "comma-separated gossip addresses")
	advertiseIP := flag.String("advertise-ip", "127.0.0.1", "IP address to advertise")

	flag.Parse()

	// addr := ":" + *port

	grpcAddr := *advertiseIP + ":" + *port // making grpc address to send in mmetadata

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("create data dir %v", err)
	}

	st, err := store.Open(store.Config{
		WALPath:  filepath.Join(*dataDir, *name+".wal"),
		BoltPath: filepath.Join(*dataDir, *name+".db"),
	})

	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	defer st.Close()

	// Rings

	r := ring.New()
	r.AddNode(*name)

	//  Creating a cluster now which is responsible for starting memberlist, attach delegate and start gossip listener
	cl, err := cluster.New(cluster.Config{
		Name:        *name,
		GossipPort:  *gossipPort,
		GRPCaddress: grpcAddr,
		Store:       st,
		Ring:        r,
	})

	if err != nil {
		log.Fatalf("create cluster: %v", err)
	}

	// Now user input  will be string and our code wants slice
	// "A,B,C" → ["A","B","C"]
	var seeds []string
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				seeds = append(seeds, p)
			}
		}
	}

	// Join cluster
	if err := cl.Join(seeds); err != nil {
		log.Fatalf("join cluster: %v", err)
	}

	srv := server.New(server.Config{
		Name:  *name,
		Store: st,
		Ring:  r,
	})

	// Start in a goroutine so we can handle signals below

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(":" + *port)
	}()

	//  Graceful shutdown
	// Wait for SIGINT (Ctrl+C) or SIGTERM (Docker stop)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case sig := <-sigCh:
		fmt.Printf("\n[%s] received %s, shutting down...\n", *name, sig)
		srv.Stop()
		_ = cl.Leave(2 * time.Second)
		_ = cl.Shutdown()
	}
	fmt.Printf("[%s] shutdown complete\n", *name)
}
