package chunkserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

	// ========================================================
	// 1. WRITE TO PRIMARY
	// ========================================================
	oldSize := c.storage.GetChunkSize(handle)

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

	newChunkSize := c.storage.GetChunkSize(handle)
	sizeChange := newChunkSize - oldSize

	// ========================================================
	// 2. FORWARD WRITE TO EVERY REPLICA
	// ========================================================

	for _, replica := range req.Replicas {

		if replica == nil {
			continue
		}

		conn, replicaClient, err :=
			connectChunkServerInfo(replica)

		if err != nil {

			// The primary is still the source of truth.
			// Try to clean up any partial replica.
			c.cleanupReplica(
				ctx,
				replica,
				req.ChunkHandle,
			)

			return &pb.WriteChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to connect to replica: " +
						err.Error(),
				},
			}, nil
		}

		forwardResp, err :=
			replicaClient.ForwardWrite(
				ctx,
				&pb.ForwardWriteRequest{
					ChunkHandle: req.ChunkHandle,
					Offset:      req.Offset,
					Data:        req.Data,
				},
			)

		conn.Close()

		if err != nil {

			c.cleanupReplica(
				ctx,
				replica,
				req.ChunkHandle,
			)

			return &pb.WriteChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to forward write: " +
						err.Error(),
				},
			}, nil
		}

		if forwardResp.Status == nil ||
			!forwardResp.Status.Success {

			c.cleanupReplica(
				ctx,
				replica,
				req.ChunkHandle,
			)

			message :=
				"replica rejected forwarded write"

			if forwardResp.Status != nil {
				message = forwardResp.Status.Message
			}

			return &pb.WriteChunkResponse{
				Status: &pb.Status{
					Success: false,
					Message: message,
				},
			}, nil
		}
	}

	// ========================================================
	// 3. ALL REPLICAS SUCCEEDED
	//
	// Now update Master metadata.
	// ========================================================

	if c.masterClient != nil {

		_, err =
			c.masterClient.UpdateMasterMetadata(
				ctx,
				&pb.UpdateMasterMetadataRequest{
					Path:   req.ChunkHandle.Path,
					Size:   sizeChange,
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
			Message: "chunk written and replicated successfully",
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

	oldSize := c.storage.GetChunkSize(handle)

	err := c.storage.TruncateChunk(
		handle,
		req.Size,
	)
	if err != nil {
		return &pb.TruncateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	if req.ReplicaOnly {
		return &pb.TruncateChunkResponse{
			Status: &pb.Status{
				Success: true,
				Message: "replica chunk truncated successfully",
			},
		}, nil
	}

	removedSize := oldSize - req.Size

	_, err = c.masterClient.UpdateMasterMetadata(
		ctx,
		&pb.UpdateMasterMetadataRequest{
			Path:   req.ChunkHandle.Path,
			Size:   removedSize,
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

func (c *ChunkServer) ForwardWrite(
	ctx context.Context,
	req *pb.ForwardWriteRequest,
) (*pb.ForwardWriteResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.ForwardWriteResponse{
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
		return &pb.ForwardWriteResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.ForwardWriteResponse{
		Status: &pb.Status{
			Success: true,
			Message: "forwarded write successful",
		},
	}, nil
}

func (c *ChunkServer) DeleteReplicaChunk(
	ctx context.Context,
	req *pb.DeleteReplicaChunkRequest,
) (*pb.DeleteReplicaChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.DeleteReplicaChunkResponse{
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
		return &pb.DeleteReplicaChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.DeleteReplicaChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "replica chunk deleted",
		},
	}, nil
}

func (c *ChunkServer) ReplicateChunk(
	ctx context.Context,
	req *pb.ReplicateChunkRequest,
) (*pb.ReplicateChunkResponse, error) {

	if req.ChunkHandle == nil {
		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk handle is required",
			},
		}, nil
	}

	if req.Destination == nil {
		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: "destination server is required",
			},
		}, nil
	}

	handle := ChunkHandle{
		Id:   req.ChunkHandle.Id,
		path: req.ChunkHandle.Path,
	}

	// --------------------------------------------------------
	// Read the complete chunk.
	// --------------------------------------------------------

	size := c.storage.GetChunkSize(handle)

	data, err := c.storage.ReadChunk(
		handle,
		0,
		size,
	)

	if err != nil {
		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	// --------------------------------------------------------
	// Connect directly to destination ChunkServer.
	// --------------------------------------------------------

	conn, destinationClient, err :=
		connectChunkServerInfo(
			req.Destination,
		)

	if err != nil {
		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	defer conn.Close()

	// --------------------------------------------------------
	// Send the complete chunk.
	//
	// ForwardWrite only writes local storage.
	// It does not update Master metadata.
	// --------------------------------------------------------

	forwardResp, err :=
		destinationClient.ForwardWrite(
			ctx,
			&pb.ForwardWriteRequest{
				ChunkHandle: req.ChunkHandle,
				Offset:      0,
				Data:        data,
			},
		)

	if err != nil {
		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	if forwardResp.Status == nil ||
		!forwardResp.Status.Success {

		message := "destination failed to receive chunk"

		if forwardResp.Status != nil {
			message = forwardResp.Status.Message
		}

		return &pb.ReplicateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: message,
			},
		}, nil
	}

	return &pb.ReplicateChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk replicated successfully",
		},
	}, nil
}

func (c *ChunkServer) cleanupReplica(
	ctx context.Context,
	replica *pb.ServerInfo,
	handle *pb.ChunkHandle,
) {

	conn, replicaClient, err :=
		connectChunkServerInfo(replica)

	if err != nil {
		return
	}

	defer conn.Close()

	_, _ =
		replicaClient.DeleteReplicaChunk(
			ctx,
			&pb.DeleteReplicaChunkRequest{
				ChunkHandle: handle,
			},
		)
}

func connectChunkServerInfo(
	server *pb.ServerInfo,
) (*grpc.ClientConn, pb.ChunkServiceClient, error) {

	if server == nil {
		return nil, nil, errors.New(
			"chunk server information is nil",
		)
	}

	address := fmt.Sprintf(
		"%s:%d",
		server.Host,
		server.Port,
	)

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return nil, nil, err
	}

	return conn, pb.NewChunkServiceClient(conn), nil
}
