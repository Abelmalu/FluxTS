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
	logger  *platform.Logger
}

func NewFluxHandler(logger *platform.Logger, sv abstract.FluxService) *FluxHandler {

	return &FluxHandler{

		service: sv,
		logger:  logger,
	}
}

func (h *FluxHandler) Write(stream grpc.BidiStreamingServer[pb.Batch, pb.Ack]) error {

	ackCh := make(chan *pb.Ack, 100)
	errCh := make(chan error, 2)

	recvDone := make(chan struct{})
	sendDone := make(chan struct{})

	go func() {

		defer close(ackCh)
		defer close(recvDone)

		for {

			req, err := stream.Recv()

			if err != nil {

				h.logger.Error(
					"Error while receiving data",
					zap.Error(err),
				)

				if err == io.EOF {

					h.logger.Info("Stream closed gracefully by client")

					return

				}

				select {

				case errCh <- err:

				default:

				}
				return

			}

			h.logger.Info(
				"Received batch",
				zap.String("message", req.String()),
			)

			ack := &pb.Ack{
				Message: "Success",
			}

			select {

			case ackCh <- ack:

			case <-stream.Context().Done():
				return
			}
		}

	}()

	go func() {

		defer close(sendDone)

		for ack := range ackCh {

			if err := stream.Send(ack); err != nil {

				select {

				case errCh <- err:
				default:
				}

				return

			}

			h.logger.Info(
				"Sent ack message",
				zap.String("message", ack.Message),
			)

		}

	}()

	select {
	case err := <-errCh:
		return err

	case <-recvDone:
		<-sendDone
		return nil
	}

}
func (h *FluxHandler) QueryRange(context.Context, *pb.QueryRangeRequest) (*pb.QueryRangeResponse, error) {

	return nil, nil
}
