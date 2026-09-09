package main

import (
	"context"
	"fmt"
	"log"

	clientpkg "github.com/Tharunqi/mini-gfs/internal/client"
)

func main() {
	ctx := context.Background()

	c, err := clientpkg.New("localhost:50051")
	if err != nil {
		log.Fatal(err)
	}

	path := "parallel_test.txt"

	fmt.Println("========================================")
	fmt.Println("PARALLEL READ/WRITE/APPEND TEST")
	fmt.Println("========================================")

	// --------------------------------------------------
	// TEST 1: CREATE
	// --------------------------------------------------

	fmt.Println("\nTEST 1: CREATE")

	if err := c.Create(ctx, path); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✅ Created:", path)

	// --------------------------------------------------
	// TEST 2: WRITE
	// --------------------------------------------------

	fmt.Println("\nTEST 2: WRITE")

	writeData := []byte(
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	)

	if err := c.Write(ctx, path, 0, writeData); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Written:", string(writeData))

	// --------------------------------------------------
	// TEST 3: APPEND
	// --------------------------------------------------

	fmt.Println("\nTEST 3: APPEND")

	appendData := []byte("APPEND")

	if err := c.Append(ctx, path, appendData); err != nil {
		log.Fatal(err)
	}

	expected := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789APPEND"

	fmt.Println("Expected:", expected)

	// --------------------------------------------------
	// TEST 4: READ
	// --------------------------------------------------

	fmt.Println("\nTEST 4: READ")

	actualData, err := c.Read(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	actual := string(actualData)

	fmt.Println("Actual:", actual)

	if actual != expected {
		log.Fatalf(
			"❌ DATA MISMATCH\nExpected: %q\nActual:   %q",
			expected,
			actual,
		)
	}

	fmt.Println("✅ DATA VERIFIED")

	fmt.Println("\n========================================")
	fmt.Println("✅ PARALLEL I/O TEST PASSED")
	fmt.Println("========================================")
}
