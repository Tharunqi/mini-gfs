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

const masterAddr = "localhost:50051"

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
	// CLEANUP
	// ============================================================

	path := "boundary_delete_test.txt"

	_, _ = master.DeleteFile(
		ctx,
		&pb.DeleteFileRequest{
			Path: path,
		},
	)

	// ============================================================
	// CREATE
	// ============================================================

	fmt.Println("\n========== CREATE ==========")

	resp, err := master.CreateFile(
		ctx,
		&pb.CreateFileRequest{
			Path: path,
		},
	)
	checkStatus("CreateFile", resp.Status, err)

	fmt.Println("Created:", path)

	// ============================================================
	// INITIAL WRITE
	// ============================================================

	fmt.Println("\n========== INITIAL WRITE ==========")

	initial := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	writeAt(
		ctx,
		master,
		path,
		0,
		initial,
	)

	fmt.Println("Initial data:", initial)

	// ============================================================
	// SHOW INITIAL METADATA
	// ============================================================

	fmt.Println("\n========== INITIAL METADATA ==========")

	printMetadata(
		ctx,
		master,
		path,
	)

	// ============================================================
	// RANGE DELETE
	// ============================================================

	fmt.Println("\n========== RANGE DELETE ==========")

	offset := uint64(10)
	length := uint64(10)

	fmt.Printf(
		"Deleting offset=%d length=%d\n",
		offset,
		length,
	)

	deleteResp, err := master.RangeDeleteFile(
		ctx,
		&pb.RangeDeleteFileRequest{
			Path:   path,
			Offset: offset,
			Length: length,
		},
	)

	checkStatus(
		"RangeDeleteFile",
		deleteResp.Status,
		err,
	)

	fmt.Printf(
		"Master returned:\n"+
			"  BeforeRange = %d\n"+
			"  Affected    = %d\n"+
			"  AfterRange  = %d\n",
		len(deleteResp.BeforeRange),
		len(deleteResp.Range),
		len(deleteResp.AfterRange),
	)

	printLocations("Before", deleteResp.BeforeRange)
	printLocations("Affected", deleteResp.Range)
	printLocations("After", deleteResp.AfterRange)

	// ============================================================
	// SEND TO CHUNK SERVER
	// ============================================================

	if len(deleteResp.Range) == 0 {
		log.Fatal("Master returned no affected chunks")
	}

	first := deleteResp.Range[0]

	if first == nil ||
		first.Primary == nil ||
		first.Handle == nil {
		log.Fatal("invalid first affected chunk location")
	}

	address := fmt.Sprintf(
		"%s:%d",
		first.Primary.Host,
		first.Primary.Port,
	)

	fmt.Println(
		"\nSending RangeDeleteChunk to:",
		address,
	)

	chunkConn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkConn.Close()

	chunkClient :=
		pb.NewChunkServiceClient(chunkConn)

	chunkResp, err :=
		chunkClient.RangeDeleteChunk(
			ctx,
			&pb.RangeDeleteChunkRequest{
				Path:             path,
				Offset:           offset,
				Length:           length,
				BeforeRange:      deleteResp.BeforeRange,
				Range:            deleteResp.Range,
				AfterRange:       deleteResp.AfterRange,
				StartOffsetRange: deleteResp.StartOffsetRange,
				EndOffsetRange:   deleteResp.EndOffsetRange,
			},
		)

	checkStatus(
		"RangeDeleteChunk",
		chunkResp.Status,
		err,
	)

	fmt.Println(
		"Chunk server:",
		chunkResp.Status.Message,
	)

	// ============================================================
	// EXPECTED RESULT
	// ============================================================

	expected :=
		initial[:offset] +
			initial[offset+length:]

	fmt.Println("\n========== EXPECTED ==========")
	fmt.Printf(
		"%q\n",
		expected,
	)

	// ============================================================
	// READ FINAL FILE
	// ============================================================

	fmt.Println("\n========== FINAL FILE ==========")

	actual, err :=
		readFile(
			ctx,
			master,
			path,
		)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"Actual:   %q\n",
		actual,
	)

	// ============================================================
	// VERIFY DATA
	// ============================================================

	fmt.Println("\n========== DATA VERIFICATION ==========")

	if actual != expected {
		fmt.Println("❌ FAILED")
		fmt.Printf("Expected: %q\n", expected)
		fmt.Printf("Actual:   %q\n", actual)

		printMetadata(
			ctx,
			master,
			path,
		)

		log.Fatal("data mismatch")
	}

	fmt.Println("✅ DATA CORRECT")

	// ============================================================
	// VERIFY METADATA
	// ============================================================

	fmt.Println("\n========== METADATA VERIFICATION ==========")

	printMetadata(
		ctx,
		master,
		path,
	)

	metadataResp, err :=
		master.OpenFile(
			ctx,
			&pb.OpenFileRequest{
				Path: path,
			},
		)

	if err != nil {
		log.Fatal(err)
	}

	expectedSize := uint64(len(expected))

	expectedChunks := 0

	if expectedSize > 0 {
		expectedChunks = int(
			(expectedSize +
				config.ChunkSize -
				1) /
				config.ChunkSize,
		)
	}

	if metadataResp.SizeBytes != expectedSize {
		log.Fatalf(
			"❌ wrong file size: expected=%d actual=%d",
			expectedSize,
			metadataResp.SizeBytes,
		)
	}

	if len(metadataResp.Chunks) != expectedChunks {
		log.Fatalf(
			"❌ wrong chunk count: expected=%d actual=%d",
			expectedChunks,
			len(metadataResp.Chunks),
		)
	}

	fmt.Println("✅ METADATA CORRECT")

	// ============================================================
	// FINAL
	// ============================================================

	fmt.Println("\n========================================")
	fmt.Println("✅ WHOLE MIDDLE CHUNK DELETE PASSED")
	fmt.Println("========================================")
}

// ================================================================
// WRITE
// ================================================================

func writeAt(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
	offset uint64,
	data string,
) {
	resp, err := master.WriteFile(
		ctx,
		&pb.WriteFileRequest{
			Path:   path,
			Offset: offset,
			Length: uint64(len(data)),
		},
	)

	checkStatus(
		"WriteFile",
		resp.Status,
		err,
	)

	writeOffset := offset
	dataOffset := 0

	for _, location := range resp.Locations {

		if location == nil ||
			location.Handle == nil ||
			location.Primary == nil {
			log.Fatal("invalid chunk location")
		}

		chunkOffset :=
			writeOffset %
				uint64(config.ChunkSize)

		capacity :=
			uint64(config.ChunkSize) -
				chunkOffset

		remaining :=
			uint64(len(data) - dataOffset)

		writeLength := remaining

		if writeLength > capacity {
			writeLength = capacity
		}

		chunkData :=
			[]byte(
				data[dataOffset : dataOffset+int(writeLength)],
			)

		address := fmt.Sprintf(
			"%s:%d",
			location.Primary.Host,
			location.Primary.Port,
		)

		chunkConn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			log.Fatal(err)
		}

		chunkClient :=
			pb.NewChunkServiceClient(chunkConn)

		writeResp, err :=
			chunkClient.WriteChunk(
				ctx,
				&pb.WriteChunkRequest{
					ChunkHandle: location.Handle,
					Offset:      chunkOffset,
					Data:        chunkData,
				},
			)

		chunkConn.Close()

		checkStatus(
			"WriteChunk",
			writeResp.Status,
			err,
		)

		fmt.Printf(
			"Chunk %d: wrote %d bytes\n",
			location.Handle.Id,
			writeLength,
		)

		dataOffset += int(writeLength)
		writeOffset += writeLength

		if dataOffset == len(data) {
			break
		}
	}

	if dataOffset != len(data) {
		log.Fatalf(
			"only wrote %d/%d bytes",
			dataOffset,
			len(data),
		)
	}
}

// ================================================================
// READ
// ================================================================

func readFile(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
) (string, error) {

	resp, err := master.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)

	if err != nil {
		return "", err
	}

	if resp.Status == nil ||
		!resp.Status.Success {
		return "",
			fmt.Errorf(
				"OpenFile failed: %s",
				resp.Status.Message,
			)
	}

	var result []byte
	remaining := resp.SizeBytes

	for i, handle := range resp.Chunks {

		if remaining == 0 {
			break
		}

		locationResp, err :=
			master.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			return "", err
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {
			return "",
				fmt.Errorf(
					"GetChunkLocations failed for %d",
					handle.Id,
				)
		}

		location := locationResp.Location

		if location == nil ||
			location.Primary == nil {
			return "",
				fmt.Errorf(
					"chunk %d has no primary",
					handle.Id,
				)
		}

		readLength := remaining

		if readLength >
			uint64(config.ChunkSize) {
			readLength =
				uint64(config.ChunkSize)
		}

		address := fmt.Sprintf(
			"%s:%d",
			location.Primary.Host,
			location.Primary.Port,
		)

		chunkConn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			return "", err
		}

		chunkClient :=
			pb.NewChunkServiceClient(chunkConn)

		readResp, err :=
			chunkClient.ReadChunk(
				ctx,
				&pb.ReadChunkRequest{
					ChunkHandle: handle,
					Offset:      0,
					Length:      readLength,
				},
			)

		chunkConn.Close()

		if err != nil {
			return "",
				fmt.Errorf(
					"ReadChunk %d: %w",
					handle.Id,
					err,
				)
		}

		if readResp.Status == nil ||
			!readResp.Status.Success {
			return "",
				fmt.Errorf(
					"ReadChunk %d failed",
					handle.Id,
				)
		}

		fmt.Printf(
			"Chunk[%d] ID=%d → %q\n",
			i,
			handle.Id,
			string(readResp.Data),
		)

		result =
			append(
				result,
				readResp.Data...,
			)

		remaining -=
			uint64(len(readResp.Data))
	}

	return string(result), nil
}

// ================================================================
// METADATA
// ================================================================

func printMetadata(
	ctx context.Context,
	master pb.MasterServiceClient,
	path string,
) {
	resp, err := master.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if resp.Status == nil ||
		!resp.Status.Success {
		log.Fatal("OpenFile failed")
	}

	fmt.Printf(
		"Size: %d\n",
		resp.SizeBytes,
	)

	fmt.Printf(
		"Chunks: %d\n",
		len(resp.Chunks),
	)

	for i, chunk := range resp.Chunks {
		fmt.Printf(
			"  logical[%d] → ID=%d\n",
			i,
			chunk.Id,
		)
	}
}

// ================================================================
// LOCATION PRINT
// ================================================================

func printLocations(
	name string,
	locations []*pb.ChunkLocation,
) {
	fmt.Printf(
		"%s:\n",
		name,
	)

	for _, location := range locations {

		if location == nil ||
			location.Handle == nil {
			continue
		}

		fmt.Printf(
			"  chunk ID=%d\n",
			location.Handle.Id,
		)
	}
}

// ================================================================
// STATUS CHECK
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
