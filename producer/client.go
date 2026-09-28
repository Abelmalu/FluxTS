package main

import (
	"os"

	"github.com/abelmalu/fluxts/platform"
	"github.com/abelmalu/fluxts/proto/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func initClient(serverAddr string, logger *platform.Logger) (pb.FluxServiceClient, *grpc.ClientConn) {

	conn, err := grpc.NewClient(

		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {

		logger.Error("dial failed", zap.String("addr", serverAddr), zap.Error(err))
		os.Exit(1)

	}


	client := pb.NewFluxServiceClient(conn)

	return client,conn
}
