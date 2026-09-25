package main

import (
	"flag"
	"fmt"
	"time"
)

type Config struct {
	Producers        int
	SamplesPerSecond int
	BatchSize        int
	FlushInterval    time.Duration
	Duration         time.Duration
	MaxInflight      int
	ServerAddr		 string
}

func parseFlags() (Config, error) {
	cfg := Config{}
	flag.StringVar(&cfg.ServerAddr, "addr", "localhost:50051", "FluxTS gRPC server address (host:port)")
	flag.IntVar(&cfg.Producers, "producers", 1, "number of concurrent producer streams")
	flag.IntVar(&cfg.SamplesPerSecond, "sps", 10, "samples per second, per simulated series")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "max samples per batch before a forced flush")
	flag.DurationVar(&cfg.FlushInterval, "flush-interval", 500*time.Millisecond, "flush at least this often")
	flag.DurationVar(&cfg.Duration, "duration", 0, "total run time (0 = until Ctrl-C)")
	flag.IntVar(&cfg.MaxInflight, "max-inflight", 1, "max unacknowledged batches per stream")
	flag.Parse()

	if cfg.ServerAddr == "" {
		return cfg, fmt.Errorf("-addr is required")
	}
	if cfg.Producers <= 0 {
		return cfg, fmt.Errorf("-producers must be > 0")
	}
	if cfg.SamplesPerSecond <= 0 {
		return cfg, fmt.Errorf("-sps must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return cfg, fmt.Errorf("-batch-size must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		return cfg, fmt.Errorf("-flush-interval must be > 0")
	}
	if cfg.MaxInflight <= 0 {
		return cfg, fmt.Errorf("-max-inflight must be > 0")
	}
	return cfg, nil
}
