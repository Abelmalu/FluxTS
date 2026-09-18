package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abelmalu/fluxts/config"
	ierrors "github.com/abelmalu/fluxts/errors"
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
	case sig := <-shutdownSignal:

		s.logger.Info("Shutdown signal received, stopping server...", zap.String("signal", sig.String()))

		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second*15)

		defer cancel()

		stopped := make(chan struct{})

		go func() {

			s.grpcServer.GracefulStop()

			close(stopped)

		}()

		select {

		case <-shutdownContext.Done():

			s.logger.Warn("Graceful shutdown timed out, forcing immediate stop")
			s.grpcServer.Stop()

			return ierrors.ErrServerShutDownTimeOut

		case <-stopped:

			s.logger.Info("Server stopped cleanly")
			return nil

		}

	}

}
