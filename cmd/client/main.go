package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	masterAddr = "localhost:50051"
	filePath   = "persistent_test.txt"
)

func main() {

	// --------------------------------------------------
	// Connect to Master
	// --------------------------------------------------

	conn, err := grpc.NewClient(
		masterAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatalf(
			"failed to connect to Master: %v",
			err,
		)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	fmt.Println("Connected to Master")

	// --------------------------------------------------
	// OPEN FILE
	// --------------------------------------------------

	resp, err := client.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: filePath,
		},
	)

	if err != nil {
		log.Fatalf(
			"OpenFile RPC failed: %v",
			err,
		)
	}

	if resp.Status == nil ||
		!resp.Status.Success {

		log.Fatalf(
			"OpenFile failed: %s",
			resp.Status.Message,
		)
	}

	fmt.Println("\n========== MASTER METADATA ==========")
	fmt.Println("Path:", filePath)
	fmt.Println("Size:", resp.SizeBytes)
	fmt.Println("Chunks:", len(resp.Chunks))

	// --------------------------------------------------
	// READ EACH CHUNK
	// --------------------------------------------------

	var result []byte

	for i, handle := range resp.Chunks {

		fmt.Printf(
			"\nReading chunk[%d] ID=%d\n",
			i,
			handle.Id,
		)

		// Ask Master where the chunk lives.
		locationResp, err :=
			client.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			log.Fatalf(
				"GetChunkLocations failed: %v",
				err,
			)
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			log.Fatalf(
				"GetChunkLocations failed: %s",
				locationResp.Status.Message,
			)
		}

		location := locationResp.Location

		if location == nil ||
			location.Primary == nil {

			log.Fatal(
				"invalid chunk location",
			)
		}

		chunkAddr := fmt.Sprintf(
			"%s:%d",
			location.Primary.Host,
			location.Primary.Port,
		)

		chunkConn, err := grpc.NewClient(
			chunkAddr,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			log.Fatalf(
				"failed to connect to Chunk Server: %v",
				err,
			)
		}

		chunkClient :=
			pb.NewChunkServiceClient(chunkConn)

		// --------------------------------------------------
		// Read chunk
		// --------------------------------------------------

		readResp, err :=
			chunkClient.ReadChunk(
				ctx,
				&pb.ReadChunkRequest{
					ChunkHandle: handle,
					Offset:      0,
					Length:      10,
				},
			)

		chunkConn.Close()

		if err != nil {
			log.Fatalf(
				"ReadChunk failed: %v",
				err,
			)
		}

		if readResp.Status == nil ||
			!readResp.Status.Success {

			log.Fatalf(
				"ReadChunk failed: %s",
				readResp.Status.Message,
			)
		}

		fmt.Printf(
			"Chunk data: %q\n",
			string(readResp.Data),
		)

		result = append(
			result,
			readResp.Data...,
		)
	}

	// --------------------------------------------------
	// FINAL RESULT
	// --------------------------------------------------

	fmt.Println("\n========== FINAL FILE ==========")
	fmt.Printf(
		"Data: %q\n",
		string(result),
	)

	fmt.Println(
		"Size:",
		len(result),
	)

	if uint64(len(result)) != resp.SizeBytes {
		log.Fatalf(
			"SIZE MISMATCH: metadata=%d actual=%d",
			resp.SizeBytes,
			len(result),
		)
	}

	fmt.Println("\n✅ READ SUCCESSFUL")
	fmt.Println("✅ MASTER METADATA RESTORED")
	fmt.Println("================================")

	_ = io.EOF // keep io import harmless if needed by generated API
}
