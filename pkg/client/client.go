package client

import (
	"context"
	"fmt"
	"time"

	"github.com/KalashThakare/distributed-kv/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.KVStoreClient // generated stub
	addr   string
}

// New connects to the server at the given address and returns a client.
// It doesn’t actually connect immediately — it waits until you make the first request.
// Example address: "localhost:8082"

func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(4*1024*1024), // 4MB message size maximum
	))

	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewKVStoreClient(conn),
		addr:   addr,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func defaultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (c *Client) Get(key string) (value string, found bool, err error) {
	ctx, cancel := defaultCtx()
	defer cancel()
	resp, err := c.client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", false, fmt.Errorf("Get %q from %s: %w", key, c.addr, err)
	}
	return resp.Value, resp.Found, nil
}

func (c *Client) Put(key, value string) error {
	ctx, cancle := defaultCtx()
	defer cancle()

	_, err := c.client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("Put %q to %s: %w", key, c.addr, err)
	}

	return nil
}

func (c *Client) Delete(key string) error {
	ctx, cancel := defaultCtx()
	defer cancel()

	_, err := c.client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("Delete %q from %s: %w", key, c.addr, err)
	}

	return nil
}

func (c *Client) GetN(key string, n int) (nodes []string, err error) {
	ctx, cancel := defaultCtx()
	defer cancel()

	resp, err := c.client.GetN(ctx, &pb.GetNRequest{Key: key, N: int32(n)})
	if err != nil {
		return nil, fmt.Errorf("GetN %q from %s: %w", key, c.addr, err)
	}

	return resp.Nodes, nil
}

func (c *Client) Health() (node_name string, key_count int, err error) {
	ctx, cancel := defaultCtx()
	defer cancel()

	resp, err := c.client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return "", 0, fmt.Errorf("Health %s: %w", c.addr, err)
	}

	return resp.NodeName, int(resp.KeyCount), nil
}

func IsNotFound(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}
