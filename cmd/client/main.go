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
		log.Fatal("Connect:", err)
	}
	defer gfs.Close()

	fmt.Println("Connected to Master")

	path := "persistence_test.txt"

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("PERSISTENCE RESTART TEST")
	fmt.Println("========================================")

	fmt.Println("File:", path)

	// --------------------------------------------------------
	// OPEN
	// --------------------------------------------------------

	info, err := gfs.Open(ctx, path)

	if err != nil {
		log.Fatal("Open:", err)
	}

	fmt.Println("Metadata size:", info.Size)
	fmt.Println("Chunk count:", len(info.Chunks))

	// --------------------------------------------------------
	// READ
	// --------------------------------------------------------

	data, err := gfs.Read(ctx, path)

	if err != nil {
		log.Fatal("Read:", err)
	}

	fmt.Println()
	fmt.Println("Recovered data:")
	fmt.Printf("%q\n", string(data))

	// --------------------------------------------------------
	// VERIFY
	// --------------------------------------------------------

	expected := "ABCDEFGHIJKLMNOPQRST"

	if string(data) != expected {
		log.Fatalf(
			"❌ DATA MISMATCH: expected %q, got %q",
			expected,
			string(data),
		)
	}

	if info.Size != uint64(len(expected)) {
		log.Fatalf(
			"❌ SIZE MISMATCH: metadata=%d expected=%d",
			info.Size,
			len(expected),
		)
	}

	fmt.Println()
	fmt.Println("✅ DATA VERIFIED")
	fmt.Println("✅ METADATA VERIFIED")
	fmt.Println("✅ PERSISTENCE RESTART TEST PASSED")
}
