package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/Tharunqi/mini-gfs/internal/master"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

func main() {

	// Listen on TCP port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create our implementation
	masterServer := master.NewMasterServer()

	// Register it with gRPC
	pb.RegisterMasterServiceServer(
		grpcServer,
		masterServer,
	)

	log.Println("Master Server listening on :50051")

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
