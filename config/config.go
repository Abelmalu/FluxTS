package config

import (
	"errors"
	"os"
)

type Config struct {
	GRPCPORT string
}

func LoadConfig() (*Config, error) {

	cfg := Config{}

	cfg.GRPCPORT = ":" + os.Getenv("GRPC_PORT")

	if cfg.GRPCPORT == "" {

		return nil, errors.New("GRPC_PORT environment variable is required!")

	}

	return &cfg, nil

}
