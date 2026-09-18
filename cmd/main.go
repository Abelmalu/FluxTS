package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	// pb "your-module/proto/generated" // Replace with your actual generated proto package
)

// Example server implementation
type server struct {
	// pb.UnimplementedYourServiceServer
}

func main() {
	// 1. Create a network listener
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 2. Initialize the gRPC server
	grpcServer := grpc.NewServer()

	// Register your service here
	// pb.RegisterYourServiceServer(grpcServer, &server{})

	// 3. Start the server in a separate goroutine so it doesn't block main
	go func() {
		log.Printf("gRPC server is running on %s", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// 4. Set up channel to listen for termination signals (Ctrl+C, Kubernetes SIGTERM, etc.)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Block until a signal is received
	sig := <-stopChan
	log.Printf("Received signal %v. Initiating graceful shutdown...", sig)

	// 5. Implement the graceful shutdown mechanism with a timeout safety net
	shutdownClosed := make(chan struct{})

	go func() {
		// GracefulStop blocks until all active RPCs are complete
		grpcServer.GracefulStop()
		close(shutdownClosed)
	}()

	// Set a maximum duration to wait for active requests to finish
	const shutdownTimeout = 10 * time.Second

	select {
	case <-shutdownClosed:
		log.Println("gRPC server stopped cleanly.")
	case <-time.After(shutdownTimeout):
		log.Printf("Shutdown timeout of %v reached. Forcing server stop.", shutdownTimeout)
		// Forcefully terminate remaining connections
		grpcServer.Stop() 
	}

	log.Println("Application exited completely.")
}
