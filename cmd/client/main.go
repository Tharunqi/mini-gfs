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
	// TEST 1: INSERT INSIDE ONE CHUNK
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 1: INSERT INSIDE CHUNK")
	fmt.Println("========================================")

	path := "insert_test_1.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	initial := []byte("ABCDEFGHIJ")

	err = gfs.Write(
		ctx,
		path,
		0,
		initial,
	)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Insert(
		ctx,
		path,
		5,
		[]byte("XYZ"),
	)
	if err != nil {
		log.Fatal("Insert:", err)
	}

	verify(
		ctx,
		gfs,
		path,
		"ABCDEXYZFGHIJ",
	)

	fmt.Println("✅ TEST 1 PASSED")

	// ========================================================
	// TEST 2: INSERT AT BEGINNING
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 2: INSERT AT BEGINNING")
	fmt.Println("========================================")

	path = "insert_test_2.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Write(
		ctx,
		path,
		0,
		[]byte("ABCDEFGHIJ"),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Insert(
		ctx,
		path,
		0,
		[]byte("XYZ"),
	)
	if err != nil {
		log.Fatal("Insert:", err)
	}

	verify(
		ctx,
		gfs,
		path,
		"XYZABCDEFGHIJ",
	)

	fmt.Println("✅ TEST 2 PASSED")

	// ========================================================
	// TEST 3: INSERT AT EOF
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 3: INSERT AT EOF")
	fmt.Println("========================================")

	path = "insert_test_3.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Write(
		ctx,
		path,
		0,
		[]byte("ABCDEFGHIJ"),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Insert(
		ctx,
		path,
		10,
		[]byte("XYZ"),
	)
	if err != nil {
		log.Fatal("Insert:", err)
	}

	verify(
		ctx,
		gfs,
		path,
		"ABCDEFGHIJXYZ",
	)

	fmt.Println("✅ TEST 3 PASSED")

	// ========================================================
	// TEST 4: CROSS-CHUNK INSERT
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 4: CROSS-CHUNK INSERT")
	fmt.Println("========================================")

	path = "insert_test_4.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	initial = []byte(
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	)

	err = gfs.Write(
		ctx,
		path,
		0,
		initial,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Insert in the middle of chunk 0.
	err = gfs.Insert(
		ctx,
		path,
		7,
		[]byte("XYZ"),
	)
	if err != nil {
		log.Fatal("Insert:", err)
	}

	verify(
		ctx,
		gfs,
		path,
		"ABCDEFGXYZHIJKLMNOPQRSTUVWXYZ0123456789",
	)

	fmt.Println("✅ TEST 4 PASSED")

	// ========================================================
	// TEST 5: INSERT LARGER THAN ONE CHUNK
	// ========================================================

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("TEST 5: LARGE INSERT")
	fmt.Println("========================================")

	path = "insert_test_5.txt"

	err = gfs.Create(ctx, path)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Write(
		ctx,
		path,
		0,
		[]byte("ABCDEFGHIJ"),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = gfs.Insert(
		ctx,
		path,
		5,
		[]byte("1234567890ABCDE"),
	)
	if err != nil {
		log.Fatal("Insert:", err)
	}

	verify(
		ctx,
		gfs,
		path,
		"ABCDE1234567890ABCDEFGHIJ",
	)

	fmt.Println("✅ TEST 5 PASSED")

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("ALL INSERT TESTS PASSED")
	fmt.Println("========================================")
}

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
