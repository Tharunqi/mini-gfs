package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Tharunqi/mini-gfs/internal/chunkserver"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	id := flag.String(
		"id",
		"chunkserver-1",
		"chunk server ID",
	)

	host := flag.String(
		"host",
		"localhost",
		"chunk server host",
	)

	port := flag.Int(
		"port",
		50052,
		"chunk server port",
	)

	flag.Parse()

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
		log.Fatalf(
			"failed to connect to master: %v",
			err,
		)
	}

	defer masterConn.Close()

	masterClient :=
		pb.NewMasterServiceClient(masterConn)

	// ---------------------------------------------------------
	// Server information
	// ---------------------------------------------------------

	serverInfo := &pb.ServerInfo{
		Id:   *id,
		Host: *host,
		Port: uint32(*port),
	}

	// ---------------------------------------------------------
	// Create Chunk Server
	// ---------------------------------------------------------

	server :=
		chunkserver.NewChunkServer(
			masterClient,
			serverInfo,
		)

	// ---------------------------------------------------------
	// Register with Master
	// ---------------------------------------------------------
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	defer cancel()

	go server.StartHeartbeat(ctx)

	err = server.RegisterWithMaster(
		ctx,
	)

	if err != nil {
		log.Fatalf(
			"failed to register with master: %v",
			err,
		)
	}

	log.Printf(
		"Registered %s with Master",
		*id,
	)

	// ---------------------------------------------------------
	// Listen for clients
	// ---------------------------------------------------------

	address :=
		fmt.Sprintf("%s:%d", *host, *port)

	lis, err := net.Listen(
		"tcp",
		address,
	)

	if err != nil {
		log.Fatalf(
			"failed to listen on %s: %v",
			address,
			err,
		)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterChunkServiceServer(
		grpcServer,
		server,
	)

	log.Printf(
		"Chunk server %s listening on %s",
		*id,
		address,
	)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf(
			"failed to serve chunk server: %v",
			err,
		)
	}
}
