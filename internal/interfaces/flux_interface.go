package abstract

import "github.com/abelmalu/fluxts/internal/model"

type FluxService interface {
	Create(batch model.Batch) error
}

type FluxRepository interface {
	Insert(batch model.Batch) error
}
