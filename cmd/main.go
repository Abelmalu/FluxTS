package main

import (
	"github.com/abelmalu/fluxts/config"
	"github.com/abelmalu/fluxts/platform"
	"github.com/abelmalu/fluxts/server"
	"go.uber.org/zap"
)

var logger = platform.InitZapLogger()

func main() {
	cfg, err := config.LoadConfig()

	if err != nil {

		logger.Error("config error", zap.Error(err))
	}

	server := server.NewServer(logger, cfg)

	if err := server.StartServer(); err != nil {

		logger.Error("Server Start Error", zap.Error(err))
	}
}
