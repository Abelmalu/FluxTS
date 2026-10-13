package model

import (
	"fmt"
	"strings"
)


type SeriesKey string

type Sample struct {
	TimestampMs int64
	Value       float64
}

type Label struct {
	Name  string
	Value string
}

type Series struct {
	Metric string
	Labels []Label
}

type Batch struct {
	Series  Series
	Samples []Sample
}

func (s Series) Key() SeriesKey {
	var b strings.Builder
	// Rough preallocation: metric + per-label overhead.
	size := len(s.Metric) + 16
	for _, l := range s.Labels {
		size += len(l.Name) + len(l.Value) + 16
	}
	b.Grow(size)

	fmt.Fprintf(&b, "%d:%s", len(s.Metric), s.Metric)
	for _, l := range s.Labels {
		fmt.Fprintf(&b, "|%d:%s=%d:%s", len(l.Name), l.Name, len(l.Value), l.Value)
	}
	return SeriesKey(b.String())
}
