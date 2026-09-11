package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Tharunqi/mini-gfs/internal/client"
	"github.com/Tharunqi/mini-gfs/internal/config"
)

func main() {
	ctx := context.Background()

	c, err := client.New("localhost:50051")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	path := "latency_test.txt"

	// 20 chunks worth of data.
	size := 5 * config.ChunkSize

	data := make([]byte, size)

	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	fmt.Println("========================================")
	fmt.Println("PARALLEL I/O LATENCY TEST")
	fmt.Println("========================================")
	fmt.Println("Chunk size:", config.ChunkSize)
	fmt.Println("File size:", len(data))
	fmt.Println("Expected chunks:", (len(data)+int(config.ChunkSize)-1)/int(config.ChunkSize))
	fmt.Println()

	// --------------------------------------------------
	// CREATE
	// --------------------------------------------------

	_ = c.Delete(ctx, path)

	err = c.Create(ctx, path)
	if err != nil {
		panic(err)
	}

	// --------------------------------------------------
	// WRITE
	// --------------------------------------------------

	fmt.Println("Writing large file...")

	start := time.Now()

	err = c.Write(ctx, path, 0, data)
	if err != nil {
		panic(err)
	}

	writeTime := time.Since(start)

	fmt.Println("Write time:", writeTime)
	fmt.Println()

	// --------------------------------------------------
	// READ 1
	// --------------------------------------------------

	fmt.Println("Reading file...")

	start = time.Now()

	result, err := c.Read(ctx, path)
	if err != nil {
		panic(err)
	}

	readTime1 := time.Since(start)

	fmt.Println("Read time:", readTime1)
	fmt.Println("Bytes read:", len(result))

	// --------------------------------------------------
	// VERIFY
	// --------------------------------------------------

	if len(result) != len(data) {
		panic(fmt.Sprintf(
			"size mismatch: expected %d, got %d",
			len(data),
			len(result),
		))
	}

	for i := range data {
		if result[i] != data[i] {
			panic(fmt.Sprintf(
				"data mismatch at byte %d",
				i,
			))
		}
	}

	fmt.Println("Data verified: OK")
	fmt.Println()

	// --------------------------------------------------
	// READ AGAIN
	// --------------------------------------------------

	fmt.Println("Reading again...")

	start = time.Now()

	result, err = c.Read(ctx, path)
	if err != nil {
		panic(err)
	}

	readTime2 := time.Since(start)

	fmt.Println("Second read time:", readTime2)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("RESULT")
	fmt.Println("========================================")
	fmt.Println("Write:", writeTime)
	fmt.Println("Read 1:", readTime1)
	fmt.Println("Read 2:", readTime2)
	fmt.Println("========================================")
}
