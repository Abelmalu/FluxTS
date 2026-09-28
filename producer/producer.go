package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/abelmalu/fluxts/platform"
	"github.com/abelmalu/fluxts/proto/pb"
	"go.uber.org/zap"
)

type Producer struct {
	id     int
	cfg    Config
	client pb.FluxServiceClient
	logger *platform.Logger
	gen    *Generator

	sent   atomic.Uint64
	acked  atomic.Uint64
	errors atomic.Uint64
}

type BatchBuffer struct {
	series  *pb.Series
	samples []*pb.Sample
}

func NewProducer(id int, cfg Config, client pb.FluxServiceClient, logger *platform.Logger) *Producer {
	return &Producer{
		id:     id,
		cfg:    cfg,
		client: client,
		logger: logger,
		gen:    NewGenerator(id),
	}
}

func (p *Producer) Run(ctx context.Context) error {
	defer func() {
		sent, acked, errs := p.Stats()
		p.logger.Info("producer exited",
			zap.Int("producer_id", p.id),
			zap.Uint64("batches_sent", sent),
			zap.Uint64("batches_acked", acked),
			zap.Uint64("send_errors", errs),
		)
	}()

	stream, err := p.client.Write(ctx)
	if err != nil {

		return fmt.Errorf("open write stream: %w", err)
	}

	inflight := make(chan string, p.cfg.MaxInflight)

	recDone := make(chan error, 1)


	go p.runReceiver(stream,inflight,recDone)

	

}

func (p *Producer) runReceiver(stream pb.FluxService_WriteClient, inflight <-chan string, done chan<- error) {

	for {

		ack, err := stream.Recv()
		if err != nil {

			done <- nil

			return

		}

		if err != nil {
			done <- err

			return
		}

		select {

		case <-inflight:
		default:
			p.logger.Warn("ack received with empty inflight window",
				zap.Int("producer_id", p.id),
				zap.String("request_id", ack.GetRequestId()),
			)

		}

		p.acked.Add(1)

	}
}

func (p *Producer) Stats() (sent, acked, errs uint64) {

	return p.sent.Load(), p.acked.Load(), p.errors.Load()
}
