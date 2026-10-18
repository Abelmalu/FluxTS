package service

import (
	abstract "github.com/abelmalu/fluxts/internal/interfaces"
	"github.com/abelmalu/fluxts/internal/model"
)

type FluxService struct {
	repo abstract.FluxRepository
}



func NewFluxService(r abstract.FluxRepository) *FluxService {


	return &FluxService{

		repo: r,
	}
}


func (s *FluxService) Create(batch model.Batch) error {


	return nil
}