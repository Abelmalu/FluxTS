package handler

import (
	"context"
	"io"

	abstract "github.com/abelmalu/fluxts/internal/interfaces"
	"github.com/abelmalu/fluxts/platform"
	"github.com/abelmalu/fluxts/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type FluxHandler struct {
	pb.UnimplementedFluxServiceServer
	service abstract.FluxService
	logger  platform.Logger
}

func NewFluxHandler(sv abstract.FluxService) *FluxHandler {

	return &FluxHandler{

		service: sv,
	}
}

func (h *FluxHandler) Write(stream grpc.BidiStreamingServer[pb.Batch, pb.Ack]) error {

	for {

		req, err := stream.Recv()

		h.logger.Info("messages",zap.String("message",req.String()))

		if err == io.EOF {
			h.logger.Error("Error EOF", zap.Error(err))

			return err

		}

		if err != nil {

			h.logger.Error("Error EOF", zap.Error(err))

			return err

		}
		err = stream.Send(&pb.Ack{
			Message: "Success",
		})
		if err != nil {
			h.logger.Error("Error sending Ack to strea", zap.Error(err))
			return err
		}

	}

}
func (h *FluxHandler) QueryRange(context.Context, *pb.QueryRangeRequest) (*pb.QueryRangeResponse, error) {

	return nil, nil
}
