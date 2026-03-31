package client

import (
	"fmt"

	"github.com/KalashThakare/distributed-kv/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	client pb.KVStoreClient // generated stub
	addr string
}

// New connects to the server at the given address and returns a client.
// It doesn’t actually connect immediately — it waits until you make the first request.
// Example address: "localhost:8082"

func  New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(4*1024*1024), // 4MB message size maximum
	),)

	if err != nil{
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return &Client{
		conn: conn,
		client: pb.NewKVStoreClient(conn),
		addr: addr,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

