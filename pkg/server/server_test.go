package server

import (
	"context"
	"net"
	"testing"

	"github.com/KalashThakare/distributed-kv/pkg/pb"
	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024 // 1MB in-process buffer
// startTestServer spins up a gRPC server over bufconn and returns a client.
// The server uses an in-memory store (no WAL, no bbolt) for speed.
// t.Cleanup() shuts everything down after the test.
func startTestServer(t *testing.T) pb.KVStoreClient {

	t.Helper()
	// bufconn listener: in-process pipe, looks like a real net.Listener
	lis := bufconn.Listen(bufSize)
	// In-memory store — no files needed in tests
	st, err := store.Open(store.Config{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	r := ring.New()
	r.AddNode("TestNode")
	srv := New(Config{Name: "TestNode", Store: st, Ring: r})
	// Start the gRPC server over bufconn
	go func() {
		if err := srv.grpcServer.Serve(lis); err != nil {
			// ignore "server stopped" error on cleanup
		}
	}()
	// Create a client that dials through bufconn (no real TCP)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		conn.Close()
		st.Close()
	})
	return pb.NewKVStoreClient(conn)
}

//Integration test

func TestServer_PutGet(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	// Put a key
	_, err := c.Put(ctx, &pb.PutRequest{Key: "user:rahul", Value: "Rahul Sharma"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Get it back
	resp, err := c.Get(ctx, &pb.GetRequest{Key: "user:rahul"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected Found=true")
	}
	if resp.Value != "Rahul Sharma" {
		t.Errorf("want %q, got %q", "Rahul Sharma", resp.Value)
	}
}
func TestServer_GetNotFound(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	resp, err := c.Get(ctx, &pb.GetRequest{Key: "nonexistent"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Found {
		t.Error("expected Found=false for missing key")
	}
	if resp.Value != "" {
		t.Errorf("expected empty value, got %q", resp.Value)
	}
}
func TestServer_Delete(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	_, _ = c.Put(ctx, &pb.PutRequest{Key: "temp", Value: "gone"})
	_, err := c.Delete(ctx, &pb.DeleteRequest{Key: "temp"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	resp, _ := c.Get(ctx, &pb.GetRequest{Key: "temp"})
	if resp.Found {
		t.Error("key should be deleted")
	}
}
func TestServer_InvalidArgument(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	// Empty key should return InvalidArgument
	_, err := c.Get(ctx, &pb.GetRequest{Key: ""})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	// Check it is the right gRPC error code
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("want InvalidArgument, got %v", err)
	}
}
func TestServer_Health(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	_, _ = c.Put(ctx, &pb.PutRequest{Key: "k1", Value: "v1"})
	_, _ = c.Put(ctx, &pb.PutRequest{Key: "k2", Value: "v2"})
	resp, err := c.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if resp.NodeName != "TestNode" {
		t.Errorf("want TestNode, got %q", resp.NodeName)
	}
	if resp.KeyCount != 2 {
		t.Errorf("want 2 keys, got %d", resp.KeyCount)
	}
}
func TestServer_GetN(t *testing.T) {
	c := startTestServer(t)
	ctx := context.Background()
	resp, err := c.GetN(ctx, &pb.GetNRequest{Key: "user:rahul", N: 1})
	if err != nil {
		t.Fatalf("GetN: %v", err)
	}
	if len(resp.Nodes) != 1 {
		t.Errorf("want 1 node, got %d", len(resp.Nodes))
	}
	t.Logf("node for user:rahul: %v", resp.Nodes)
}
