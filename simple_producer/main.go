package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abelmalu/fluxts/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "FluxTS server address")
	flag.Parse()

	conn, err := grpc.NewClient(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewFluxServiceClient(conn)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	stream, err := client.Write(ctx)
	if err != nil {
		log.Fatalf("open Write stream: %v", err)
	}


	go func() {
		for {
			ack, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				log.Println("server closed the stream")
				return
			}
			if err != nil {
				log.Printf("recv error: %v", err)
				return
			}
			log.Printf("ack: request_id=%q accepted=%d msg=%q",
				ack.GetRequestId(), ack.GetAcceptedCount(), ack.GetMessage())
		}
	}()

	series := &pb.Series{
		Metrics: "cpu_usage",
		Labels: []*pb.Label{
			{Name: "instance", Value: "loadgen-0"},
		},
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			
			log.Println("shutting down")
			_ = stream.CloseSend()
			time.Sleep(200 * time.Millisecond)
			return

		case now := <-ticker.C:
			seq++
			batch := &pb.Batch{
				RequestId: newRequestID(seq),
				Series:    series,
				Samples: []*pb.Sample{
					{
						Timestamp: timestamppb.New(now),
						Value:     fakeCPU(now.Sub(start), rng),
					},
				},
			}
			if err := stream.Send(batch); err != nil {
				log.Printf("send error: %v", err)
				return
			}
			log.Printf("sent batch %s (value=%.2f)",
				batch.RequestId, batch.Samples[0].Value)
		}
	}
}


func fakeCPU(since time.Duration, rng *rand.Rand) float64 {
	const period = 30 * time.Second
	phase := 2 * math.Pi * since.Seconds() / period.Seconds()
	return 45 + 20*math.Sin(phase) + rng.NormFloat64()*2
}

func newRequestID(seq int) string {
	return "req-" + itoa(seq)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
