package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

func main() {

	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	resp, err := client.CreateFile(
		context.Background(),
		&pb.CreateFileRequest{
			Path: "/notes.txt",
		},
	)
	resp2, err2 := client.DeleteFile(
		context.Background(),
		&pb.DeleteFileRequest{
			Path: "/notes.txt",
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	if err2 != nil {
		log.Fatal(err2)
	}

	log.Println(resp.Status.Message)
	log.Println(resp2.Status.Message)
}
