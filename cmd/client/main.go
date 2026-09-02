package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Tharunqi/mini-gfs/internal/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gfs, err := client.New("localhost:50051")
	if err != nil {
		log.Fatal(err)
	}
	defer gfs.Close()

	path := "persistence_test.txt"

	data, err := gfs.Read(ctx, path)
	if err != nil {
		log.Fatal("Read:", err)
	}

	expected := "ABCDEFGHIJKLMNOPQRST"

	fmt.Println("========================================")
	fmt.Println("READ TEST")
	fmt.Println("========================================")

	fmt.Printf("Expected: %q\n", expected)
	fmt.Printf("Actual:   %q\n", string(data))

	if string(data) != expected {
		log.Fatalf("❌ DATA MISMATCH")
	}

	fmt.Printf("Size: %d bytes\n", len(data))
	fmt.Println("✅ DATA VERIFIED")
	fmt.Println("✅ READ TEST PASSED")
}
