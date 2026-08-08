package main

import (
	"log"
	"net"

	"github.com/Tharunqi/mini-gfs/internal/chunkserver"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// ---------------------------------------------------------
	// Connect to Master
	// ---------------------------------------------------------

	masterConn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatalf("failed to connect to master: %v", err)
	}
	defer masterConn.Close()

	masterClient := pb.NewMasterServiceClient(masterConn)

	// ---------------------------------------------------------
	// Create Chunk Server
	// ---------------------------------------------------------

	server := chunkserver.NewChunkServer(masterClient)

	// ---------------------------------------------------------
	// Listen for Chunk Server clients
	// ---------------------------------------------------------

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen on :50052: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterChunkServiceServer(
		grpcServer,
		server,
	)

	log.Println("Chunk server listening on :50052")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve chunk server: %v", err)
	}
}
