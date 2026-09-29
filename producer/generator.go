package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/abelmalu/fluxts/proto/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Generator struct {
	series []simMetric
	rng    *rand.Rand
	start  time.Time
}

type simMetric struct {
	metric string
	labels []*pb.Label
	next   func(now time.Time, since time.Duration, rng *rand.Rand) float64
}

type Series struct {
	series simMetric
	sample *pb.Sample
}	


func NewGenerator(id int) *Generator {
	start := time.Now()
	labels := []*pb.Label{
		{Name: "instance", Value: fmt.Sprintf("loadgen-%d", id)},
	}

	rng := rand.New(rand.NewSource(int64(id+1) * time.Now().UnixNano()))

	return &Generator{
		start: start,
		rng:   rng,
		series: []simMetric{
			{
				metric: "cpu_usage",
				labels: labels,
				next:   sineWave(45, 20, 60*time.Second, 0.05),
			},
			{
				metric: "memory_bytes",
				labels: labels,
				next: func(_ time.Time, since time.Duration, r *rand.Rand) float64 {
					base := 100_000_000.0 + since.Seconds()*1_000_000.0
					return base + r.NormFloat64()*500_000.0
				},
			},
			{
				metric: "active_connections",
				labels: labels,
				next:   sineWave(150, 40, 30*time.Second, 0.1),
			},
		},
	}
}

func (g *Generator) Tick(now time.Time) []Series {

	since := now.Sub(g.start)

	out := make([]Series, 0, len(g.series))

	for _, s := range g.series {
		v := s.next(now, since, g.rng)

		out = append(out, Series{
			series: s,
			sample: &pb.Sample{

				Timestamp: timestamppb.New(now),
				Value:     v,
			},
		})
	}


	return out

}

func sineWave(base, amplitude float64, period time.Duration, noiseFrac float64) func(time.Time, time.Duration, *rand.Rand) float64 {
	periodSec := period.Seconds()
	return func(_ time.Time, since time.Duration, r *rand.Rand) float64 {
		phase := 2 * math.Pi * since.Seconds() / periodSec
		return base + amplitude*math.Sin(phase) + r.NormFloat64()*amplitude*noiseFrac
	}
}
