package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Tharunqi/mini-gfs/internal/config"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()
	path := "hello.txt"

	// =========================================================
	// Connect to Master
	// =========================================================

	masterConn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer masterConn.Close()

	masterClient := pb.NewMasterServiceClient(masterConn)

	fmt.Println("Connected to Master")

	// =========================================================
	// CREATE FILE
	// =========================================================

	fmt.Println("\n========== CREATE FILE ==========")

	createResp, err := masterClient.CreateFile(
		ctx,
		&pb.CreateFileRequest{
			Path: path,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !createResp.Status.Success {
		log.Fatal(createResp.Status.Message)
	}

	fmt.Println("Created:", path)

	// =========================================================
	// TEST 1: WRITE ACROSS MULTIPLE CHUNKS
	//
	// Chunk size = 10
	//
	// Data = "ABCDEFGHIJKLM"
	// Length = 13
	//
	// Chunk 0 → ABCDEFGHIJ
	// Chunk 1 → KLM
	// =========================================================

	fmt.Println("\n========== TEST 1: MULTI-CHUNK WRITE ==========")

	data := []byte("ABCDEFGHIJKLM")

	writeResp, err := masterClient.WriteFile(
		ctx,
		&pb.WriteFileRequest{
			Path:   path,
			Offset: 0,
			Length: uint64(len(data)),
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !writeResp.Status.Success {
		log.Fatal(writeResp.Status.Message)
	}

	fmt.Printf(
		"Master returned %d chunk locations\n",
		len(writeResp.Locations),
	)

	err = writeDataToChunks(
		ctx,
		writeResp.Locations,
		0,
		data,
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Multi-chunk write successful")

	// =========================================================
	// TEST 2: OVERWRITE ACROSS CHUNK BOUNDARY
	//
	// Existing:
	//
	// Chunk 0 → ABCDEFGHIJ
	// Chunk 1 → KLM
	//
	// Write "XYZ123" at offset 7
	//
	// Chunk 0:
	// ABCDEFGXYZ
	//
	// Chunk 1:
	// 123
	// =========================================================

	fmt.Println("\n========== TEST 2: CROSS-CHUNK OVERWRITE ==========")

	data = []byte("XYZ123")
	offset := uint64(7)

	writeResp, err = masterClient.WriteFile(
		ctx,
		&pb.WriteFileRequest{
			Path:   path,
			Offset: offset,
			Length: uint64(len(data)),
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !writeResp.Status.Success {
		log.Fatal(writeResp.Status.Message)
	}

	fmt.Printf(
		"Master returned %d chunk locations\n",
		len(writeResp.Locations),
	)

	err = writeDataToChunks(
		ctx,
		writeResp.Locations,
		offset,
		data,
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Cross-chunk overwrite successful")

	// =========================================================
	// TEST 3: SIMPLE APPEND
	//
	// Append "HELLO"
	//
	// Master decides the offset.
	// =========================================================

	fmt.Println("\n========== TEST 3: APPEND ==========")

	appendData := []byte("HELLO")

	appendResp, err := masterClient.AppendFile(
		ctx,
		&pb.AppendFileRequest{
			Path:   path,
			Length: uint64(len(appendData)),
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !appendResp.Status.Success {
		log.Fatal(appendResp.Status.Message)
	}

	fmt.Println("Append offset:", appendResp.Offset)

	fmt.Printf(
		"Master returned %d chunk locations\n",
		len(appendResp.Locations),
	)

	err = writeDataToChunks(
		ctx,
		appendResp.Locations,
		appendResp.Offset,
		appendData,
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Append successful")

	// =========================================================
	// TEST 4: MULTI-CHUNK APPEND
	//
	// Append 25 bytes.
	//
	// This should span multiple chunks with ChunkSize = 10.
	// =========================================================

	fmt.Println("\n========== TEST 4: MULTI-CHUNK APPEND ==========")

	appendData = []byte("1234567890123456789012345")

	appendResp, err = masterClient.AppendFile(
		ctx,
		&pb.AppendFileRequest{
			Path:   path,
			Length: uint64(len(appendData)),
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !appendResp.Status.Success {
		log.Fatal(appendResp.Status.Message)
	}

	fmt.Println("Append offset:", appendResp.Offset)

	fmt.Printf(
		"Master returned %d chunk locations\n",
		len(appendResp.Locations),
	)

	err = writeDataToChunks(
		ctx,
		appendResp.Locations,
		appendResp.Offset,
		appendData,
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Multi-chunk append successful")

	// =========================================================
	// FINAL OPEN
	// =========================================================

	fmt.Println("\n========== FINAL FILE ==========")

	openResp, err := masterClient.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if !openResp.Status.Success {
		log.Fatal(openResp.Status.Message)
	}

	fmt.Println("File size:", openResp.SizeBytes)
	fmt.Println("Number of chunks:", len(openResp.Chunks))

	for i, chunk := range openResp.Chunks {
		fmt.Printf(
			"Chunk %d → ID=%d Path=%s\n",
			i,
			chunk.Id,
			chunk.Path,
		)
	}
}

// =============================================================
// CLIENT-SIDE DATA SPLITTING
//
// This uses the EXISTING WriteChunk RPC.
// =============================================================

func writeDataToChunks(
	ctx context.Context,
	locations []*pb.ChunkLocation,
	fileOffset uint64,
	data []byte,
) error {

	dataOffset := 0

	for _, location := range locations {

		if location == nil {
			return fmt.Errorf("nil chunk location")
		}

		if location.Handle == nil {
			return fmt.Errorf("chunk location has no handle")
		}

		if location.Primary == nil {
			return fmt.Errorf(
				"chunk %d has no primary",
				location.Handle.Id,
			)
		}

		// Offset inside the current chunk.
		chunkOffset := fileOffset % config.ChunkSize

		// Remaining space in this chunk.
		remainingChunkSpace :=
			config.ChunkSize - chunkOffset

		// Remaining data.
		remainingData :=
			uint64(len(data) - dataOffset)

		writeLength := remainingData

		if writeLength > remainingChunkSpace {
			writeLength = remainingChunkSpace
		}

		chunkData := data[dataOffset : dataOffset+int(writeLength)]

		fmt.Printf(
			"Writing %d bytes → chunk %d at offset %d\n",
			writeLength,
			location.Handle.Id,
			chunkOffset,
		)

		// -----------------------------------------------------
		// Connect to primary Chunk Server
		// -----------------------------------------------------

		address := fmt.Sprintf(
			"%s:%d",
			location.Primary.Host,
			location.Primary.Port,
		)

		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			return fmt.Errorf(
				"failed to connect to chunk server %s: %w",
				address,
				err,
			)
		}

		chunkClient := pb.NewChunkServiceClient(conn)

		// -----------------------------------------------------
		// EXISTING WriteChunk RPC
		// -----------------------------------------------------

		writeResp, err := chunkClient.WriteChunk(
			ctx,
			&pb.WriteChunkRequest{
				ChunkHandle: location.Handle,
				Offset:      chunkOffset,
				Data:        chunkData,
			},
		)

		conn.Close()

		if err != nil {
			return fmt.Errorf(
				"WriteChunk failed for chunk %d: %w",
				location.Handle.Id,
				err,
			)
		}

		if writeResp == nil ||
			writeResp.Status == nil ||
			!writeResp.Status.Success {

			return fmt.Errorf(
				"chunk %d write failed",
				location.Handle.Id,
			)
		}

		fmt.Printf(
			"Chunk %d write successful\n",
			location.Handle.Id,
		)

		dataOffset += int(writeLength)
		fileOffset += writeLength

		if dataOffset == len(data) {
			break
		}
	}

	if dataOffset != len(data) {
		return fmt.Errorf(
			"only wrote %d/%d bytes",
			dataOffset,
			len(data),
		)
	}

	return nil
}
