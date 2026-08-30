package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tharunqi/mini-gfs/internal/master"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
)

func main() {

	// --------------------------------------------------
	// 1. Create metadata store
	// --------------------------------------------------

	metadata := master.NewMetadataStore()

	// --------------------------------------------------
	// 2. Load metadata from disk
	// --------------------------------------------------

	if err := metadata.Load(); err != nil {
		log.Fatalf(
			"failed to load metadata: %v",
			err,
		)
	}

	log.Println("Metadata loaded successfully")

	// --------------------------------------------------
	// 3. Create Master gRPC server
	// --------------------------------------------------

	masterServer := master.NewMasterServer(
		metadata,
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	go masterServer.StartFailureDetector(ctx)

	go masterServer.StartReplicationManager(ctx)

	grpcServer := grpc.NewServer()

	pb.RegisterMasterServiceServer(
		grpcServer,
		masterServer,
	)

	// --------------------------------------------------
	// 4. Start listening
	// --------------------------------------------------

	listener, err := net.Listen(
		"tcp",
		":50051",
	)
	if err != nil {
		log.Fatalf(
			"failed to listen on :50051: %v",
			err,
		)
	}

	log.Println("Master listening on :50051")

	// --------------------------------------------------
	// 5. Start gRPC server in background
	// --------------------------------------------------

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf(
				"gRPC server stopped: %v",
				err,
			)
		}
	}()

	// --------------------------------------------------
	// 6. Wait for shutdown signal
	// --------------------------------------------------

	stop := make(chan os.Signal, 1)

	signal.Notify(
		stop,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-stop

	log.Println("Shutting down Master...")

	// --------------------------------------------------
	// 7. Stop accepting new RPCs
	// --------------------------------------------------

	grpcServer.GracefulStop()

	// --------------------------------------------------
	// 8. Save metadata to disk
	// --------------------------------------------------

	if err := metadata.Save(); err != nil {
		log.Printf(
			"failed to save metadata: %v",
			err,
		)
	} else {
		log.Println("Metadata saved successfully")
	}

	log.Println("Master stopped")
}
