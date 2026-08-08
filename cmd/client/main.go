package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// =========================================================
	// 1. CONNECT TO MASTER
	// =========================================================

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

	fmt.Println("Connected to Master")

	// =========================================================
	// 2. CREATE FILE
	// =========================================================

	filename := "hello.txt"

	createResp, err := masterClient.CreateFile(
		ctx,
		&pb.CreateFileRequest{
			Path: filename,
		},
	)
	if err != nil {
		log.Fatalf("CreateFile failed: %v", err)
	}

	if !createResp.Status.Success {
		log.Fatalf(
			"CreateFile failed: %s",
			createResp.Status.Message,
		)
	}

	fmt.Println("File created:", filename)

	// =========================================================
	// 3. ALLOCATE CHUNK
	// =========================================================

	allocResp, err := masterClient.AllocateChunk(
		ctx,
		&pb.AllocateChunkRequest{
			Path: filename,
		},
	)
	if err != nil {
		log.Fatalf("AllocateChunk failed: %v", err)
	}

	if !allocResp.Status.Success {
		log.Fatalf(
			"AllocateChunk failed: %s",
			allocResp.Status.Message,
		)
	}

	location := allocResp.Location

	fmt.Printf(
		"Allocated chunk: %d\n",
		location.Handle.Id,
	)

	fmt.Printf(
		"Chunk server: %s:%d\n",
		location.Primary.Host,
		location.Primary.Port,
	)

	// =========================================================
	// 4. CONNECT TO CHUNK SERVER
	// =========================================================

	chunkServerAddress := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	chunkConn, err := grpc.NewClient(
		chunkServerAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatalf(
			"failed to connect to chunk server: %v",
			err,
		)
	}
	defer chunkConn.Close()

	chunkClient := pb.NewChunkServiceClient(chunkConn)

	fmt.Println(
		"Connected to Chunk Server:",
		chunkServerAddress,
	)

	// =========================================================
	// 5. WRITE CHUNK
	// =========================================================

	data := []byte("Hello World")

	writeResp, err := chunkClient.WriteChunk(
		ctx,
		&pb.WriteChunkRequest{
			ChunkHandle: location.Handle,
			Offset:      0,
			Data:        data,
		},
	)
	if err != nil {
		log.Fatalf("WriteChunk failed: %v", err)
	}

	if !writeResp.Status.Success {
		log.Fatalf(
			"WriteChunk failed: %s",
			writeResp.Status.Message,
		)
	}

	fmt.Println("Write successful")

	// =========================================================
	// 6. OPEN FILE
	// =========================================================

	openResp, err := masterClient.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: filename,
		},
	)
	if err != nil {
		log.Fatalf("OpenFile failed: %v", err)
	}

	if !openResp.Status.Success {
		log.Fatalf(
			"OpenFile failed: %s",
			openResp.Status.Message,
		)
	}

	fmt.Printf(
		"File size: %d bytes\n",
		openResp.SizeBytes,
	)

	fmt.Printf(
		"Number of chunks: %d\n",
		len(openResp.Chunks),
	)

	// =========================================================
	// 7. GET CHUNK LOCATION
	// =========================================================

	if len(openResp.Chunks) == 0 {
		log.Fatal("file has no chunks")
	}

	chunkHandle := openResp.Chunks[0]

	locationResp, err := masterClient.GetChunkLocations(
		ctx,
		&pb.GetChunkLocationsRequest{
			ChunkHandle: chunkHandle,
		},
	)
	if err != nil {
		log.Fatalf(
			"GetChunkLocation failed: %v",
			err,
		)
	}

	if !locationResp.Status.Success {
		log.Fatalf(
			"GetChunkLocation failed: %s",
			locationResp.Status.Message,
		)
	}

	location = locationResp.Location

	fmt.Printf(
		"Chunk %d is on %s:%d\n",
		chunkHandle.Id,
		location.Primary.Host,
		location.Primary.Port,
	)

	// =========================================================
	// 8. READ CHUNK
	// =========================================================

	readResp, err := chunkClient.ReadChunk(
		ctx,
		&pb.ReadChunkRequest{
			ChunkHandle: chunkHandle,
			Offset:      0,
			Length:      openResp.SizeBytes,
		},
	)
	if err != nil {
		log.Fatalf("ReadChunk failed: %v", err)
	}

	if !readResp.Status.Success {
		log.Fatalf(
			"ReadChunk failed: %s",
			readResp.Status.Message,
		)
	}

	fmt.Println(
		"Read data:",
		string(readResp.Data),
	)

	fmt.Println("Read successful")
}
