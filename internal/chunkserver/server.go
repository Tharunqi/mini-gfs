package chunkserver

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

type ChunkServer struct {
	pb.UnimplementedChunkServiceServer

	storage      *Storage
	masterClient pb.MasterServiceClient
}

func NewChunkServer(
	masterClient pb.MasterServiceClient,
) *ChunkServer {
	return &ChunkServer{
		storage:      NewStorage(),
		masterClient: masterClient,
	}
}

// ============================================================
// WRITE
// ============================================================

func (c *ChunkServer) WriteChunk(
	ctx context.Context,
	req *pb.WriteChunkRequest,
) (*pb.WriteChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.WriteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk handle is required",
			},
		}, nil
	}

	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}

	err := c.storage.WriteChunk(
		handle,
		req.Offset,
		req.Data,
	)

	if err != nil {
		return &pb.WriteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	if c.masterClient != nil {
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
					Message: "failed to update master metadata",
				},
			}, nil
		}
	}

	return &pb.WriteChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk written successfully",
		},
	}, nil
}

// ============================================================
// READ
// ============================================================

func (c *ChunkServer) ReadChunk(
	ctx context.Context,
	req *pb.ReadChunkRequest,
) (*pb.ReadChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.ReadChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk handle is required",
			},
		}, nil
	}

	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}

	data, err := c.storage.ReadChunk(
		handle,
		req.Offset,
		req.Length,
	)

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

// ============================================================
// DELETE CHUNK
// ============================================================

func (c *ChunkServer) DeleteChunk(
	ctx context.Context,
	req *pb.DeleteChunkRequest,
) (*pb.DeleteChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.DeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk handle is required",
			},
		}, nil
	}

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

	return &pb.DeleteChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk deleted successfully",
		},
	}, nil
}

// ============================================================
// TRUNCATE = RANGE DELETE
// ============================================================

func (c *ChunkServer) RangeDeleteChunk(
	ctx context.Context,
	req *pb.RangeDeleteChunkRequest,
) (*pb.RangeDeleteChunkResponse, error) {

	if req == nil {
		return &pb.RangeDeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "request is nil",
			},
		}, nil
	}

	if req.Length == 0 {
		return &pb.RangeDeleteChunkResponse{
			Status: &pb.Status{
				Success: true,
				Message: "nothing to delete",
			},
		}, nil
	}

	// --------------------------------------------------------
	// Build the complete ordered list of handles.
	//
	// BeforeRange is NOT rewritten.
	// But we need its handles to calculate the absolute
	// index of the affected region.
	// --------------------------------------------------------

	allLocations := make(
		[]*pb.ChunkLocation,
		0,
		len(req.BeforeRange)+
			len(req.Range)+
			len(req.AfterRange),
	)

	allLocations = append(
		allLocations,
		req.BeforeRange...,
	)

	allLocations = append(
		allLocations,
		req.Range...,
	)

	allLocations = append(
		allLocations,
		req.AfterRange...,
	)

	handles := make(
		[]ChunkHandle,
		0,
		len(allLocations),
	)

	for _, location := range allLocations {

		if location == nil ||
			location.Handle == nil {
			continue
		}

		handles = append(
			handles,
			ChunkHandle{
				Id:   location.Handle.Id,
				path: location.Handle.Path,
			},
		)
	}

	if len(handles) == 0 {
		return &pb.RangeDeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "no chunks supplied",
			},
		}, nil
	}

	fmt.Printf(
		"[ChunkServer] range delete: path=%s offset=%d length=%d\n",
		req.Path,
		req.Offset,
		req.Length,
	)
	survivingChunks, newSize, err :=
		c.storage.RangeDeleteChunk(
			handles,
			req.Offset,
			req.Length,
		)

	if err != nil {
		return &pb.RangeDeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}
	chunkHandles := make(
		[]*pb.ChunkHandle,
		0,
		len(survivingChunks),
	)

	for _, id := range survivingChunks {
		chunkHandles = append(
			chunkHandles,
			&pb.ChunkHandle{
				Id:   id,
				Path: req.Path,
			},
		)
	}
	_, err = c.masterClient.UpdateMasterMetadata(
		ctx,
		&pb.UpdateMasterMetadataRequest{
			Path:    req.Path,
			NewSize: newSize,
			Chunks:  chunkHandles,
		},
	)

	if err != nil {
		return &pb.RangeDeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "range deletion succeeded but failed to update master metadata",
			},
		}, nil
	}
	return &pb.RangeDeleteChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "range deletion and metadata update successful",
		},
	}, nil
}
