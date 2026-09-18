package server

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/abelmalu/fluxts/config"
	"github.com/abelmalu/fluxts/platform"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server
	logger     *platform.Logger
	cfg        *config.Config
}

func NewServer(logger *platform.Logger, cfg *config.Config) *Server {

	grpcServer := grpc.NewServer()

	return &Server{
		logger:     logger,
		grpcServer: grpcServer,
		cfg:        cfg,
	}
}

func (s *Server) StartServer() error {

	lis, err := net.Listen("tcp", s.cfg.GRPCPORT)
	if err != nil {
		s.logger.Error("failed to listen tcp requests", zap.Error(err))
		return err
	}

	serverErrors := make(chan error, 1)
	go func() {

		s.logger.Info("Starting server on port:", zap.String("port", s.cfg.GRPCPORT))

		if err := s.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			s.logger.Error("failed to serve", zap.Error(err))

			serverErrors <- err

		}

	}()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	select {

	case err := <-serverErrors:
		return err
	case sig := <- shutdownSignal:
 
		s.logger.Info("Shutdown signal received, stopping server...", zap.String("signal", sig.String()))
		
		s.grpcServer.GracefulStop()
		return nil

		
	}

}
