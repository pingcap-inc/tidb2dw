package owner

import (
	"context"
	"fmt"
	"net"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// GrpcServer is the generic tcp server which provide protobuf rpc service.
type GrpcServer struct {
	// The server address and port.
	addr string
	port int

	// The listener to listen to the server.
	listener *net.Listener

	// The internal grpc server.
	InternalServer *grpc.Server
}

// NewGrpcServer creates a new TcpServer.
func NewGrpcServer(addr string, port int) *GrpcServer {
	return &GrpcServer{
		addr:           addr,
		port:           port,
		InternalServer: grpc.NewServer(),
	}
}

// Start starts the server.
func (s *GrpcServer) Start(eg *errgroup.Group, _ctx context.Context) error {
	// Start to listen to the server.
	addr := fmt.Sprintf("%s:%d", s.addr, s.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.listener = &lis

	// Start the server.
	eg.Go(func() error {
		if err := s.InternalServer.Serve(*s.listener); err != nil {
			return err
		}
		return nil
	})

	return nil
}

// Stop stops the server.
func (s *GrpcServer) Stop() error {
	s.InternalServer.Stop()
	(*s.listener).Close()
	return nil
}
