package repository

import (
	"sync"

	"github.com/abelmalu/fluxts/internal/model"
)

type FluxRepository struct {
	mu     sync.RWMutex
	series map[model.SeriesKey]*seriesEntry
}

type seriesEntry struct {
	series  model.Series
	samples []model.Sample
}

func NewFluxRepository() *FluxRepository {

	return &FluxRepository{

		series: make(map[model.SeriesKey]*seriesEntry),
	}
}



func (fr *FluxRepository) Insert(batch model.Batch ) error {


	return nil
}
