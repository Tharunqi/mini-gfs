package chunkserver

import (
	"context"
	"errors"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

type ChunkServer struct {
	pb.UnimplementedChunkServiceServer

	storage      *Storage
	masterClient pb.MasterServiceClient
}

func NewChunkServer(masterClient pb.MasterServiceClient) *ChunkServer {
	return &ChunkServer{
		storage:      NewStorage(),
		masterClient: masterClient,
	}
}

func (c *ChunkServer) WriteChunk(
	ctx context.Context,
	req *pb.WriteChunkRequest,
) (*pb.WriteChunkResponse, error) {
	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}
	err := c.storage.WriteChunk(handle, req.Offset, req.Data)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return &pb.WriteChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "chunk not found",
				},
			}, nil
		}
		return &pb.WriteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	_, err = c.masterClient.UpdateChunkMetadata(
		ctx,
		&pb.UpdateChunkMetadataRequest{
			Handle:       req.ChunkHandle,
			Offset:       req.Offset,
			BytesWritten: uint64(len(req.Data)),
		},
	)
	if err != nil {
		return &pb.WriteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "failed to update chunk metadata",
			},
		}, nil
	}
	return &pb.WriteChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk written successfully",
		},
	}, nil
}

func (c *ChunkServer) ReadChunk(
	ctx context.Context,
	req *pb.ReadChunkRequest,
) (*pb.ReadChunkResponse, error) {
	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}
	data, err := c.storage.ReadChunk(handle, req.Offset, req.Length)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return &pb.ReadChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "chunk not found",
				},
			}, nil
		}
		return &pb.ReadChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ReadChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk read successfully",
		},
		Data: data,
	}, nil
}

func (c *ChunkServer) DeleteChunk(
	ctx context.Context,
	req *pb.DeleteChunkRequest,
) (*pb.DeleteChunkResponse, error) {
	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}
	err := c.storage.DeleteChunk(handle)
	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return &pb.DeleteChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "chunk not found",
				},
			}, nil
		}
		return &pb.DeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	_, err = c.masterClient.UpdateChunkMetadata(
		ctx,
		&pb.UpdateChunkMetadataRequest{
			Handle:       req.ChunkHandle,
			Offset:       0,
			BytesWritten: 0,
		},
	)
	if err != nil {
		return &pb.DeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "failed to update chunk metadata",
			},
		}, nil
	}

	return &pb.DeleteChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk deleted successfully",
		},
	}, nil
}
