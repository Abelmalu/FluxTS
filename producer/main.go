package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/abelmalu/fluxts/platform"
	"go.uber.org/zap"
)

var logger = platform.InitZapLogger()

func main() {

	Cfg, err := parseFlags()
	if err != nil {
		logger.Error("invalid configuration", zap.Error(err))
		os.Exit(2)
	}

	client, conn := initClient(Cfg.ServerAddr, logger)

	defer conn.Close()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCtx := rootCtx

	if Cfg.Duration > 0 {

		var cancel context.CancelFunc

		runCtx, cancel = context.WithTimeout(rootCtx, Cfg.Duration)

		defer cancel()

	}

	logger.Info("starting producers",
		zap.String("addr", Cfg.ServerAddr),
		zap.Int("producers", Cfg.Producers),
		zap.Int("sps", Cfg.SamplesPerSecond),
		zap.Int("batch_size", Cfg.BatchSize),
		zap.Duration("flush_interval", Cfg.FlushInterval),
		zap.Int("max_inflight", Cfg.MaxInflight),
		zap.Duration("duration", Cfg.Duration),
	)

	producers := make([]*Producer, Cfg.Producers)
	var wg sync.WaitGroup

	for i := range Cfg.Producers {
		wg.Add(1)
		p := NewProducer(i, Cfg, client, logger)
		producers[i] = p
		go func(id int, p *Producer) {

			defer wg.Done()

			if err := p.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {

				logger.Error("producer stopped with error",
					zap.Int("producer_id", id),
					zap.Error(err),
				)

			}

		}(i, p)

		
	}

	stats := make(chan struct{})
		statsDone := make(chan struct{})

		go func() {

			defer close(statsDone)

			t := time.NewTicker(300 * time.Millisecond)

			defer t.Stop()

			for {

				select {

				case <-stats:
					return
				case <-t.C:

					logAggregateStats(producers)
				}
			}

		}()

		wg.Wait()
		close(stats)
		<-statsDone
		logAggregateStats(producers)
		logger.Info("all producers stopped")


}

func logAggregateStats(producers []*Producer) {
	var sent, acked, errs uint64
	for _, p := range producers {
		s, a, e := p.Stats()
		sent += s
		acked += a
		errs += e
	}
	logger.Info("stats",
		zap.Uint64("batches_sent", sent),
		zap.Uint64("batches_acked", acked),
		zap.Uint64("send_errors", errs),
	)
}
