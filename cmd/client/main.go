package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Tharunqi/mini-gfs/internal/client"
)

func main() {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	gfs, err := client.New("localhost:50051")
	if err != nil {
		log.Fatal(err)
	}
	defer gfs.Close()

	fmt.Println("Connected to Master")

	// ========================================================
	// TEST 1: CREATE + WRITE
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 1: CREATE + WRITE")
	fmt.Println("========================================")

	path := "persistence_test.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal("Create:", err)
	}

	fmt.Println("Created:", path)

	data := []byte(
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	)

	err = gfs.Write(
		ctx,
		path,
		0,
		data,
	)

	if err != nil {
		log.Fatal("Write:", err)
	}

	fmt.Println("Written:", string(data))

	verify(
		ctx,
		gfs,
		path,
		string(data),
	)

	fmt.Println("✅ TEST 1 PASSED")

	// ========================================================
	// TEST 2: APPEND
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 2: APPEND")
	fmt.Println("========================================")

	appendData := []byte("APPEND")

	err = gfs.Append(
		ctx,
		path,
		appendData,
	)

	if err != nil {
		log.Fatal("Append:", err)
	}

	expected := string(data) + string(appendData)

	verify(
		ctx,
		gfs,
		path,
		expected,
	)

	fmt.Println("✅ TEST 2 PASSED")

	// ========================================================
	// TEST 3: TRUNCATE
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 3: TRUNCATE")
	fmt.Println("========================================")

	truncateSize := uint64(20)

	err = gfs.Truncate(
		ctx,
		path,
		truncateSize,
	)

	if err != nil {
		log.Fatal("Truncate:", err)
	}

	expected = expected[:truncateSize]

	verify(
		ctx,
		gfs,
		path,
		expected,
	)

	fmt.Println("✅ TEST 3 PASSED")

	// ========================================================
	// TEST 4: INSERT
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 4: INSERT")
	fmt.Println("========================================")

	insertData := []byte("XYZ")

	insertOffset := uint64(5)

	err = gfs.Insert(
		ctx,
		path,
		insertOffset,
		insertData,
	)

	if err != nil {
		log.Fatal("Insert:", err)
	}

	expected =
		expected[:insertOffset] +
			string(insertData) +
			expected[insertOffset:]

	verify(
		ctx,
		gfs,
		path,
		expected,
	)

	fmt.Println("✅ TEST 4 PASSED")

	// ========================================================
	// TEST 5: RANGE DELETE
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 5: RANGE DELETE")
	fmt.Println("========================================")

	// Delete 3 bytes starting at offset 5.
	deleteStart := uint64(5)

	err = gfs.DeleteRange(
		ctx,
		path,
		deleteStart,
		3,
	)

	if err != nil {
		log.Fatal("DeleteRange:", err)
	}

	expected =
		expected[:deleteStart] +
			expected[deleteStart+3:]

	verify(
		ctx,
		gfs,
		path,
		expected,
	)

	fmt.Println("✅ TEST 5 PASSED")

	// ========================================================
	// FINAL
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("ALL PERSISTENCE PRE-RESTART TESTS PASSED")
	fmt.Println("========================================")

	fmt.Println()
	fmt.Println("Now stop the Master and ChunkServer.")
	fmt.Println("Restart both servers.")
	fmt.Println("Then run this client again using the READ test below.")
}

// ============================================================
// VERIFY
// ============================================================

func verify(
	ctx context.Context,
	gfs *client.Client,
	path string,
	expected string,
) {

	data, err := gfs.Read(ctx, path)

	if err != nil {
		log.Fatal("Read:", err)
	}

	actual := string(data)

	fmt.Println()
	fmt.Println("Expected:")
	fmt.Printf("%q\n", expected)

	fmt.Println("Actual:")
	fmt.Printf("%q\n", actual)

	if actual != expected {

		log.Fatalf(
			"❌ DATA MISMATCH: expected %q, got %q",
			expected,
			actual,
		)
	}

	info, err := gfs.Open(ctx, path)

	if err != nil {
		log.Fatal("Open:", err)
	}

	fmt.Printf(
		"Metadata size: %d\n",
		info.Size,
	)

	fmt.Printf(
		"Expected size: %d\n",
		len(expected),
	)

	if info.Size != uint64(len(expected)) {

		log.Fatalf(
			"❌ SIZE MISMATCH: metadata=%d expected=%d",
			info.Size,
			len(expected),
		)
	}

	fmt.Println("✅ DATA + METADATA VERIFIED")
}
