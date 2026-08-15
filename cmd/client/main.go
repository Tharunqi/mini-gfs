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

const (
	masterAddr = "localhost:50051"
	filePath   = "truncate_test.txt"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.NewClient(
		masterAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	master := pb.NewMasterServiceClient(conn)

	fmt.Println("Connected to Master")

	// ============================================================
	// TEST 1: TRUNCATE INSIDE CHUNK
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("TEST 1: TRUNCATE INSIDE CHUNK")
	fmt.Println("========================================")

	createFreshFile(ctx, master)

	initial := []byte(
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	)

	writeFile(
		ctx,
		master,
		filePath,
		initial,
	)

	fmt.Println("\n========== INITIAL FILE ==========")

	verifyFile(
		ctx,
		master,
		filePath,
		initial,
	)

	// 36 -> 23
	newSize := uint64(23)

	fmt.Printf(
		"\nTruncating from %d → %d\n",
		len(initial),
		newSize,
	)

	truncateFile(
		ctx,
		master,
		filePath,
		newSize,
	)

	expected := initial[:newSize]

	fmt.Println("\n========== AFTER TRUNCATE ==========")

	verifyFile(
		ctx,
		master,
		filePath,
		expected,
	)

	fmt.Println("✅ TEST 1 PASSED")

	// ============================================================
	// TEST 2: EXACT CHUNK BOUNDARY
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("TEST 2: TRUNCATE AT CHUNK BOUNDARY")
	fmt.Println("========================================")

	createFreshFile(ctx, master)

	writeFile(
		ctx,
		master,
		filePath,
		initial,
	)

	newSize = uint64(config.ChunkSize * 2)

	fmt.Printf(
		"\nTruncating to %d bytes\n",
		newSize,
	)

	truncateFile(
		ctx,
		master,
		filePath,
		newSize,
	)

	expected = initial[:newSize]

	verifyFile(
		ctx,
		master,
		filePath,
		expected,
	)

	fmt.Println("✅ TEST 2 PASSED")

	// ============================================================
	// TEST 3: TRUNCATE TO ZERO
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("TEST 3: TRUNCATE TO ZERO")
	fmt.Println("========================================")

	createFreshFile(ctx, master)

	writeFile(
		ctx,
		master,
		filePath,
		initial,
	)

	truncateFile(
		ctx,
		master,
		filePath,
		0,
	)

	verifyFile(
		ctx,
		master,
		filePath,
		[]byte{},
	)

	fmt.Println("✅ TEST 3 PASSED")

	// ============================================================
	// TEST 4: TRUNCATE TO CURRENT SIZE
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("TEST 4: TRUNCATE TO CURRENT SIZE")
	fmt.Println("========================================")

	createFreshFile(ctx, master)

	writeFile(
		ctx,
		master,
		filePath,
		initial,
	)

	truncateFile(
		ctx,
		master,
		filePath,
		uint64(len(initial)),
	)

	verifyFile(
		ctx,
		master,
		filePath,
		initial,
	)

	fmt.Println("✅ TEST 4 PASSED")

	// ============================================================
	// TEST 5: INVALID TRUNCATE
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("TEST 5: INVALID TRUNCATE")
	fmt.Println("========================================")

	createFreshFile(ctx, master)

	writeFile(
		ctx,
		master,
		filePath,
		initial,
	)

	invalidSize := uint64(len(initial) + 10)

	resp, err := master.TruncateFile(
		ctx,
		&pb.TruncateFileRequest{
			Path: filePath,
			Size: invalidSize,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if resp == nil {
		log.Fatal("TruncateFile returned nil response")
	}

	if resp.Status == nil {
		log.Fatal("TruncateFile returned nil status")
	}

	if resp.Status.Success {
		log.Fatal(
			"truncate beyond file size should have failed",
		)
	}

	fmt.Println(
		"Correctly rejected:",
		resp.Status.Message,
	)

	fmt.Println("✅ TEST 5 PASSED")

	fmt.Println("\n========================================")
	fmt.Println("ALL TRUNCATE TESTS PASSED")
	fmt.Println("========================================")
}

// ================================================================
// CREATE FRESH FILE
// ================================================================

func createFreshFile(
	ctx context.Context,
	master pb.MasterServiceClient,
) {
	// Remove previous version if it exists.
	_, _ = master.DeleteFile(
		ctx,
		&pb.DeleteFileRequest{
			Path: filePath,
		},
	)

	resp, err := master.CreateFile(
		ctx,
		&pb.CreateFileRequest{
			Path: filePath,
		},
	)

	checkStatus(
		"CreateFile",
		resp.Status,
		err,
	)

	fmt.Println("Created:", filePath)
}

// ================================================================
// INITIAL WRITE
// ================================================================

func writeFile(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
	data []byte,
) {
	resp, err := master.WriteFile(
		ctx,
		&pb.WriteFileRequest{
			Path:   path,
			Offset: 0,
			Length: uint64(len(data)),
		},
	)

	checkStatus(
		"WriteFile",
		resp.Status,
		err,
	)

	currentOffset := uint64(0)
	dataPos := 0

	for _, location := range resp.Locations {

		if location == nil ||
			location.Handle == nil {
			log.Fatal("invalid chunk location")
		}

		chunkOffset :=
			currentOffset %
				uint64(config.ChunkSize)

		capacity :=
			uint64(config.ChunkSize) -
				chunkOffset

		remaining :=
			uint64(len(data) - dataPos)

		n := remaining

		if n > capacity {
			n = capacity
		}

		chunkData :=
			data[dataPos : dataPos+int(n)]

		fmt.Printf(
			"Writing %d bytes → chunk %d offset %d\n",
			n,
			location.Handle.Id,
			chunkOffset,
		)

		writeChunk(
			ctx,
			location,
			chunkOffset,
			chunkData,
		)

		dataPos += int(n)
		currentOffset += n

		if dataPos == len(data) {
			break
		}
	}

	if dataPos != len(data) {
		log.Fatalf(
			"write incomplete: %d/%d bytes",
			dataPos,
			len(data),
		)
	}
}

// ================================================================
// TRUNCATE FILE
// ================================================================

func truncateFile(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
	newSize uint64,
) {
	// ------------------------------------------------------------
	// ASK MASTER FOR TRUNCATE PLAN
	// ------------------------------------------------------------

	resp, err := master.TruncateFile(
		ctx,
		&pb.TruncateFileRequest{
			Path: path,
			Size: newSize,
		},
	)

	if err != nil {
		log.Fatalf(
			"TruncateFile RPC failed: %v",
			err,
		)
	}

	if resp == nil {
		log.Fatal("TruncateFile returned nil response")
	}

	if resp.Status == nil {
		log.Fatal("TruncateFile returned nil status")
	}

	if !resp.Status.Success {
		log.Fatalf(
			"TruncateFile failed: %s",
			resp.Status.Message,
		)
	}

	fmt.Println("\nMaster returned truncate plan:")

	fmt.Printf(
		"  DeleteChunks   = %d\n",
		len(resp.DeleteChunks),
	)

	if resp.TruncateChunk != nil &&
		resp.TruncateChunk.Handle != nil {

		fmt.Printf(
			"  TruncateChunk  = %d\n",
			resp.TruncateChunk.Handle.Id,
		)

		fmt.Printf(
			"  New chunk size = %d\n",
			resp.TruncateChunkSize,
		)
	} else {
		fmt.Println("  TruncateChunk  = none")
	}

	// ------------------------------------------------------------
	// DELETE UNNEEDED CHUNKS
	// ------------------------------------------------------------

	for _, location := range resp.DeleteChunks {

		if location == nil {
			log.Fatal(
				"Master returned nil delete location",
			)
		}

		if location.Handle == nil {
			log.Fatal(
				"delete location has nil handle",
			)
		}

		fmt.Printf(
			"Deleting chunk %d\n",
			location.Handle.Id,
		)

		deleteChunk(
			ctx,
			location,
		)
	}

	// ------------------------------------------------------------
	// TRUNCATE FINAL SURVIVING CHUNK
	// ------------------------------------------------------------

	if resp.TruncateChunk != nil {

		if resp.TruncateChunk.Handle == nil {
			log.Fatal(
				"truncate location has nil handle",
			)
		}

		fmt.Printf(
			"Truncating chunk %d → size %d\n",
			resp.TruncateChunk.Handle.Id,
			resp.TruncateChunkSize,
		)

		truncateChunk(
			ctx,
			resp.TruncateChunk,
			resp.TruncateChunkSize,
		)
	}

	fmt.Println(
		"Truncate operations completed successfully",
	)
}

// ================================================================
// TRUNCATE CHUNK
// ================================================================

func truncateChunk(
	ctx context.Context,
	location *pb.ChunkLocation,
	size uint64,
) {
	if location == nil ||
		location.Primary == nil ||
		location.Handle == nil {

		log.Fatal("invalid truncate chunk location")
	}

	addr := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)

	resp, err := client.TruncateChunk(
		ctx,
		&pb.TruncateChunkRequest{
			ChunkHandle: location.Handle,
			Size:        size,
		},
	)

	checkStatus(
		"TruncateChunk",
		resp.Status,
		err,
	)
}

// ================================================================
// DELETE CHUNK
// ================================================================

func deleteChunk(
	ctx context.Context,
	location *pb.ChunkLocation,
) {
	if location == nil ||
		location.Primary == nil ||
		location.Handle == nil {

		log.Fatal("invalid delete chunk location")
	}

	addr := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)

	resp, err := client.DeleteChunk(
		ctx,
		&pb.DeleteChunkRequest{
			ChunkHandle: location.Handle,
		},
	)

	checkStatus(
		"DeleteChunk",
		resp.Status,
		err,
	)
}

// ================================================================
// VERIFY FILE
// ================================================================

func verifyFile(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
	expected []byte,
) {
	resp, err := master.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)

	checkStatus(
		"OpenFile",
		resp.Status,
		err,
	)

	fmt.Printf(
		"Metadata size: %d\n",
		resp.SizeBytes,
	)

	fmt.Printf(
		"Metadata chunks: %d\n",
		len(resp.Chunks),
	)

	var actual []byte

	remaining := resp.SizeBytes

	for i, handle := range resp.Chunks {

		if remaining == 0 {
			break
		}

		if handle == nil {
			log.Fatal("nil chunk handle in metadata")
		}

		locationResp, err :=
			master.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			log.Fatal(err)
		}

		checkStatus(
			"GetChunkLocations",
			locationResp.Status,
			nil,
		)

		if locationResp.Location == nil {
			log.Fatal("nil chunk location")
		}

		n := remaining

		if n > uint64(config.ChunkSize) {
			n = uint64(config.ChunkSize)
		}

		data := readChunk(
			ctx,
			locationResp.Location,
			0,
			n,
		)

		fmt.Printf(
			"chunk[%d] ID=%d → %q\n",
			i,
			handle.Id,
			string(data),
		)

		actual = append(
			actual,
			data...,
		)

		remaining -= uint64(len(data))
	}

	fmt.Printf(
		"\nExpected: %q\n",
		string(expected),
	)

	fmt.Printf(
		"Actual:   %q\n",
		string(actual),
	)

	if string(actual) != string(expected) {
		log.Fatal("❌ DATA MISMATCH")
	}

	if resp.SizeBytes != uint64(len(expected)) {
		log.Fatalf(
			"❌ SIZE MISMATCH: metadata=%d expected=%d",
			resp.SizeBytes,
			len(expected),
		)
	}

	fmt.Println("✅ DATA + METADATA VERIFIED")
}

// ================================================================
// READ CHUNK
// ================================================================

func readChunk(
	ctx context.Context,
	location *pb.ChunkLocation,
	offset uint64,
	length uint64,
) []byte {
	if location == nil ||
		location.Primary == nil ||
		location.Handle == nil {

		log.Fatal("invalid chunk location")
	}

	addr := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)

	resp, err := client.ReadChunk(
		ctx,
		&pb.ReadChunkRequest{
			ChunkHandle: location.Handle,
			Offset:      offset,
			Length:      length,
		},
	)

	checkStatus(
		"ReadChunk",
		resp.Status,
		err,
	)

	return resp.Data
}

// ================================================================
// WRITE CHUNK
// ================================================================

func writeChunk(
	ctx context.Context,
	location *pb.ChunkLocation,
	offset uint64,
	data []byte,
) {
	if location == nil ||
		location.Primary == nil ||
		location.Handle == nil {

		log.Fatal("invalid chunk location")
	}

	addr := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewChunkServiceClient(conn)

	resp, err := client.WriteChunk(
		ctx,
		&pb.WriteChunkRequest{
			ChunkHandle: location.Handle,
			Offset:      offset,
			Data:        data,
		},
	)

	checkStatus(
		"WriteChunk",
		resp.Status,
		err,
	)
}

// ================================================================
// STATUS
// ================================================================

func checkStatus(
	name string,
	status *pb.Status,
	err error,
) {
	if err != nil {
		log.Fatalf(
			"%s RPC failed: %v",
			name,
			err,
		)
	}

	if status == nil {
		log.Fatalf(
			"%s returned nil status",
			name,
		)
	}

	if !status.Success {
		log.Fatalf(
			"%s failed: %s",
			name,
			status.Message,
		)
	}
}
