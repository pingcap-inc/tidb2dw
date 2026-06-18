package owner

import (
	"context"

	"github.com/pingcap-inc/tidb2dw/pkg/owner/config"
	"github.com/pingcap-inc/tidb2dw/pkg/owner/meta"
	"golang.org/x/sync/errgroup"
)

// Server is the owner server.
type Server struct {
	cfg *config.OwnerConfig

	// gRPC server
	grpcServer *GrpcServer

	// backend
	backend  meta.Backend
	campaign *Campaign

	// errgroup and context
	eg     *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(cfg *config.OwnerConfig) (*Server, error) {
	grpcServer := NewGrpcServer(cfg.Host, cfg.Port)
	backend, err := meta.NewRdsBackend(&cfg.MetaDB)
	if err != nil {
		return nil, err
	}
	campaign := NewCampaign(cfg, backend)

	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	return &Server{
		cfg:        cfg,
		grpcServer: grpcServer,
		backend:    backend,
		campaign:   campaign,
		eg:         eg,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Prepare bootstraps the backend and starts the owner campaign.
func (s *Server) Prepare() error {
	// Initialize the backend meta schema.
	if err := s.backend.Bootstrap(); err != nil {
		return err
	}

	// Start to campaign the owner.
	s.campaign.Start(s.eg, s.ctx)

	// Todo:
	// Register the owner service to the gRPC server.

	return nil
}

// Start starts the owner server.
func (s *Server) Start() error {
	return s.grpcServer.Start(s.eg, s.ctx)
}

// Stop stops the owner server.
func (s *Server) Stop() error {
	s.cancel()
	return s.grpcServer.Stop()
}

// Wait waits for the owner server to stop.
func (s *Server) Wait() error {
	return s.eg.Wait()
}
