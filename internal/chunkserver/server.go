package chunkserver

import (
	"context"
	"errors"
	"log"
	"time"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

type ChunkServer struct {
	pb.UnimplementedChunkServiceServer

	storage      *Storage
	masterClient pb.MasterServiceClient
	serverInfo   *pb.ServerInfo
}

func NewChunkServer(
	masterClient pb.MasterServiceClient,
	serverInfo *pb.ServerInfo,
) *ChunkServer {

	return &ChunkServer{
		storage: NewStorage(
			"data/" + serverInfo.Id,
		),

		masterClient: masterClient,
		serverInfo:   serverInfo,
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
		_, err = c.masterClient.UpdateMasterMetadata(
			ctx,
			&pb.UpdateMasterMetadataRequest{
				Path:   req.ChunkHandle.Path,
				Size:   uint64(len(req.Data)),
				Chunk:  req.ChunkHandle,
				Delete: 2,
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

	deleted_size := c.storage.GetChunkSize(handle)

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

	_, err = c.masterClient.UpdateMasterMetadata(
		ctx,
		&pb.UpdateMasterMetadataRequest{
			Path:   req.ChunkHandle.Path,
			Size:   deleted_size,
			Chunk:  req.ChunkHandle,
			Delete: 0,
		},
	)

	if err != nil {
		return &pb.DeleteChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "failed to update master metadata",
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

func (c *ChunkServer) TruncateChunk(
	ctx context.Context,
	req *pb.TruncateChunkRequest,
) (*pb.TruncateChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.TruncateChunkResponse{
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

	//Error handling have to do
	deleted_size := c.storage.GetChunkSize(handle) - req.Size

	err := c.storage.TruncateChunk(handle, req.Size)

	if err != nil {
		if errors.Is(err, ErrChunkNotFound) {
			return &pb.TruncateChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "chunk not found",
				},
			}, nil
		}

		return &pb.TruncateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	_, err = c.masterClient.UpdateMasterMetadata(
		ctx,
		&pb.UpdateMasterMetadataRequest{
			Path:   req.ChunkHandle.Path,
			Size:   deleted_size,
			Chunk:  req.ChunkHandle,
			Delete: 1,
		},
	)

	if err != nil {
		return &pb.TruncateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "failed to update master metadata",
			},
		}, nil
	}

	return &pb.TruncateChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk truncated successfully",
		},
	}, nil
}

func (c *ChunkServer) RegisterWithMaster(
	ctx context.Context,
) error {

	resp, err := c.masterClient.RegisterChunkServer(
		ctx,
		&pb.RegisterChunkServerRequest{
			Server: c.serverInfo,
		},
	)

	if err != nil {
		return err
	}

	if resp.Status == nil ||
		!resp.Status.Success {

		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("chunk server registration failed")
	}

	return nil
}
func (c *ChunkServer) sendHeartbeat(
	ctx context.Context,
) error {

	chunks :=
		c.storage.GetChunkHandles()

	resp, err :=
		c.masterClient.Heartbeat(
			ctx,
			&pb.HeartbeatRequest{
				Server: c.serverInfo,

				Chunks: chunks,

				AvailableSpace: 0,
			},
		)

	if err != nil {
		return err
	}

	if resp.Status == nil ||
		!resp.Status.Success {

		if resp.Status != nil {
			return errors.New(
				resp.Status.Message,
			)
		}

		return errors.New(
			"heartbeat failed",
		)
	}

	return nil
}
func (c *ChunkServer) StartHeartbeat(
	ctx context.Context,
) {

	// Send one immediately after startup.
	err := c.sendHeartbeat(ctx)

	if err != nil {
		log.Printf(
			"Initial heartbeat failed: %v",
			err,
		)
	}

	ticker := time.NewTicker(
		5 * time.Second,
	)

	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():
			return

		case <-ticker.C:

			err := c.sendHeartbeat(ctx)

			if err != nil {
				log.Printf(
					"Heartbeat failed: %v",
					err,
				)
			}
		}
	}
}
