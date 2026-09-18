package server

import (
	"github.com/abelmalu/fluxts/config"
	"github.com/abelmalu/fluxts/platform"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

type Server struct {
	router *gin.Engine
	logger *platform.Logger
	cfg    *config.Config
}

func NewServer(router *gin.Engine, logger *platform.Logger, cfg *config.Config) *Server {
	return &Server{
		router: router,
		logger: logger,
		cfg:    cfg,
	}
}

func (s *Server) StartServer() error {

	s.logger.Info("Starting server on port:", zap.String("port", s.cfg.GRPCPORT))
	return s.router.Run()
}
