package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/server"
	"github.com/KalashThakare/distributed-kv/pkg/store"
)

func main() {
	// Flags
	port := flag.String("port", "8082", "gRPC listen port")
	name := flag.String("name", "NodeA", "node name (unique in cluster)")
	dataDir := flag.String("data-dir", "/temp", "directory for WAL and bbolt files")

	flag.Parse()

	addr := ":" + *port

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

	srv := server.New(server.Config{
		Name: *name,
		Store: st,
		Ring:  r,
	})

	// Start in a goroutine so we can handle signals below

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(addr)
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
	}
	fmt.Printf("[%s] shutdown complete\n", *name)

}
