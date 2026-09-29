package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

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

type Batch struct {
	series  *pb.Series
	samples []*pb.Sample
}



var inflight chan string

var recDone = make(chan error, 1)

var seq = 0

func NewProducer(id int, cfg Config, client pb.FluxServiceClient, logger *platform.Logger) *Producer {

	inflight = make(chan string, cfg.MaxInflight)
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

	go p.runReceiver(stream, inflight, recDone)

	buffers := map[string]*Batch{}

	sampleInterval := time.Second / time.Duration(p.cfg.SamplesPerSecond)
	if sampleInterval <= 0 {
		sampleInterval = time.Millisecond
	}
	sampleTicker := time.NewTicker(sampleInterval)
	defer sampleTicker.Stop()

	flushTicker := time.NewTicker(p.cfg.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case rerr := <-recDone:

			if rerr != nil {
				p.errors.Add(1)
			}
			return rerr
		case now := <-sampleTicker.C:

			for _, bt := range p.gen.Tick(now) {

				b, ok := buffers[bt.series.metric]

				if !ok {

					b = &Batch{

						series: &pb.Series{

							Metrics: bt.series.metric,
							Labels:  bt.series.labels,
						},

						samples: make([]*pb.Sample, 0, p.cfg.BatchSize),
					}
					buffers[bt.series.metric] = b
				}
				b.samples = append(b.samples, bt.sample)

				if len(b.samples) >= p.cfg.BatchSize {

					if err := p.flushOne(b, ctx, stream); err != nil {

						return err
					}
				}

			}

		}
	}

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

func (p *Producer) flushOne(b *Batch, ctx context.Context, stream pb.FluxService_WriteClient) error {

	if len(b.samples) == 0 {

		return nil
	}

	select {

	case inflight <- "sent":
	case <-ctx.Done():

		return ctx.Err()

	}

	msg := &pb.Batch{

		RequestId: p.GenerateRequestID(),
		Series:    b.series,
		Samples:   b.samples,
	}

	if err := stream.Send(msg); err != nil {

		<-inflight
		p.errors.Add(1)

		return err

	}
	p.sent.Add(1)
	b.samples = make([]*pb.Sample, 0, p.cfg.BatchSize)

	return nil

}

func (p *Producer) GenerateRequestID() string {

	seq++

	return fmt.Sprintf("p%d-%d", p.id, seq)

}
