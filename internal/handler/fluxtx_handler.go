package handler

import (
	"context"

	abstract "github.com/abelmalu/fluxts/internal/interfaces"
	"github.com/abelmalu/fluxts/proto/pb"
	"google.golang.org/grpc"
)

type FluxHandler struct {
	pb.UnimplementedFluxServiceServer
	service abstract.FluxService
}

func (h *FluxHandler) Write(grpc.BidiStreamingServer[pb.Batch, pb.Ack]) error {

	

	return nil 
}
func (h *FluxHandler) QueryRange(context.Context, *pb.QueryRangeRequest) (*pb.QueryRangeResponse, error)
