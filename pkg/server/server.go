package server

import (
	"container/ring"
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/KalashThakare/distributed-kv/pkg/pb"
	"github.com/KalashThakare/distributed-kv/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedKVStoreServer
	name       string
	store      *store.Store
	ring       *ring.Ring
	grpcServer *grpc.Server
}

type Config struct {
	Name  string       // Node name
	Store *store.Store // storage engine
	Ring  *ring.Ring   // hash ring
}

// New for creating a new server

func New(cfg Config) *Server {
	s := &Server{
		name:  cfg.Name,
		store: cfg.Store,
		ring:  cfg.Ring,
	}

	s.grpcServer = grpc.NewServer()

	pb.RegisterKVStoreServer(s.grpcServer, s)

	return s
}

// Start listening server on port 8082

func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Listen %s: %w", addr, err)
	}

	fmt.Printf("[%s] gRPC server listening on %s\n", s.name, addr)

	return s.grpcServer.Serve(lis)
}

// Stop gracefully shuts down the gRPC server.

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

//============= GetRequest gRPC handler================

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Validate input — never trust the caller
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key must not be empty")
	}

	val, err := s.store.Get(req.Key)
	if err != nil{
		if errors.Is(err, store.ErrKeyNotFound){
			// Return NOT_FOUND with the found=false flag.
			return &pb.GetResponse{Found: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "get %q: %v", req.Key, err)
	}

	return &pb.GetResponse{Value: val, Found: true}, nil

}

//============= PutRequest gRPC handler================

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key must not be empty")
	}

	if err := s.store.Put(req.Key, req.Value); err != nil {
		return nil, status.Errorf(codes.Internal, "put %q: %v", req.Key, err)
	}

	return &pb.PutResponse{}, nil
}

//============= DeleteRequest gRPC handler================

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == ""{
		return nil, status.Error(codes.InvalidArgument, "Key must not be empty")
	}

	if err := s.store.Delete(req.Key); err != nil{
		return nil, status.Errorf(codes.Internal, "Delete %q: %v", req.Key, err)
	}

	return &pb.DeleteResponse{}, nil
}

