package master

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

type MasterServer struct {
	pb.UnimplementedMasterServiceServer

	metadata *MetadataStore

	replicationManager *ReplicationManager
}

func NewMasterServer(metadata *MetadataStore) *MasterServer {
	server := &MasterServer{
		metadata: metadata,
	}

	server.replicationManager = NewReplicationManager(metadata)

	return server
}

func (m *MasterServer) CreateFile(
	ctx context.Context,
	req *pb.CreateFileRequest,
) (*pb.CreateFileResponse, error) {
	// Business logic
	err := m.metadata.CreateFile(req.Path)

	// Convert business errors to protobuf response
	if err != nil {

		if errors.Is(err, ErrFileExists) {
			return &pb.CreateFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "file already exists",
				},
			}, nil
		}

		return &pb.CreateFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	// Success
	return &pb.CreateFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "file created successfully",
		},
	}, nil
}

func (m *MasterServer) DeleteFile(
	ctx context.Context,
	req *pb.DeleteFileRequest,
) (*pb.DeleteFileResponse, error) {
	err := m.metadata.DeleteFile(req.Path)

	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return &pb.DeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "file not found",
				},
			}, nil
		}

		return &pb.DeleteFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.DeleteFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "file deleted successfully",
		},
	}, nil
}

func (m *MasterServer) OpenFile(
	ctx context.Context,
	req *pb.OpenFileRequest,
) (*pb.OpenFileResponse, error) {
	filemetadata, err := m.metadata.OpenFile(req.Path)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return &pb.OpenFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "file not found",
				},
			}, nil
		}
		return &pb.OpenFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	chunks := make([]*pb.ChunkHandle, 0, len(filemetadata.ChunkHandles))

	for _, id := range filemetadata.ChunkHandles {
		chunks = append(chunks, &pb.ChunkHandle{
			Id:   id,
			Path: req.Path,
		})
	}

	return &pb.OpenFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "file opened successfully",
		},
		SizeBytes: filemetadata.SizeBytes,
		Chunks:    chunks,
	}, nil
}

func (m *MasterServer) GetChunkLocations(
	ctx context.Context,
	req *pb.GetChunkLocationsRequest,
) (*pb.GetChunkLocationsResponse, error) {

	chunk, err :=
		m.metadata.GetChunkMetadata(
			req.ChunkHandle.Id,
		)

	if err != nil {
		return &pb.GetChunkLocationsResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	// Check whether the primary server is currently available.
	if chunk.Primary == nil {
		return &pb.GetChunkLocationsResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk has no primary server",
			},
		}, nil
	}

	if !m.metadata.IsChunkServerAvailable(
		chunk.Primary.ID,
	) {
		return &pb.GetChunkLocationsResponse{
			Status: &pb.Status{
				Success: false,
				Message: "chunk server is unavailable",
			},
		}, nil
	}

	location :=
		chunkLocationFromMetadata(chunk)

	return &pb.GetChunkLocationsResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk locations retrieved successfully",
		},

		Location: location,
	}, nil
}

func (m *MasterServer) AllocateChunk(
	ctx context.Context,
	req *pb.AllocateChunkRequest,
) (*pb.AllocateChunkResponse, error) {
	chunkHandle, err := m.metadata.AllocateChunk(req.Path, req.Index)
	if err != nil {
		return &pb.AllocateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}
	server, err := m.metadata.AllocateChunkServer(3)
	primary := server[0]
	replica1 := server[1]
	replica2 := server[2]

	if err != nil {
		return &pb.AllocateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	err = m.metadata.SetChunkPrimary(
		chunkHandle,
		primary,
	)
	if err != nil {
		return &pb.AllocateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}
	err = m.metadata.SetChunkReplica(
		chunkHandle,
		replica1,
	)

	if err != nil {
		return &pb.AllocateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	err = m.metadata.SetChunkReplica(
		chunkHandle,
		replica2,
	)

	if err != nil {
		return &pb.AllocateChunkResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	locationResp, err :=
		m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   chunkHandle,
					Path: req.Path,
				},
			},
		)
	return &pb.AllocateChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk allocated successfully",
		},
		Location: locationResp.Location,
	}, nil
}

func (m *MasterServer) WriteFile(
	ctx context.Context,
	req *pb.WriteFileRequest,
) (*pb.WriteFileResponse, error) {

	chunkIDs, err := m.metadata.WriteFile(
		req.Path,
		req.Offset,
		req.Length,
	)

	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return &pb.WriteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "file not found",
				},
			}, nil
		}

		return &pb.WriteFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	locations := make(
		[]*pb.ChunkLocation,
		0,
		len(chunkIDs),
	)

	for _, chunkID := range chunkIDs {

		chunk, err :=
			m.metadata.GetChunkMetadata(chunkID)

		if err != nil {
			// return error response
		}

		// If this chunk has no primary,
		// choose one now.
		if chunk.Primary == nil {

			server, err :=
				m.metadata.AllocateChunkServer(3)

			if err != nil {
				// return error response
			}

			primary := server[0]
			replica1 := server[1]
			replica2 := server[2]

			err = m.metadata.SetChunkPrimary(
				chunkID,
				primary,
			)

			if err != nil {
				return &pb.WriteFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}
			err = m.metadata.SetChunkReplica(
				chunkID,
				replica1,
			)

			if err != nil {
				return &pb.WriteFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}

			err = m.metadata.SetChunkReplica(
				chunkID,
				replica2,
			)

			if err != nil {
				return &pb.WriteFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}
		}
		locationResp, err :=
			m.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: &pb.ChunkHandle{
						Id:   chunkID,
						Path: req.Path,
					},
				},
			)

		if err != nil {
			return &pb.WriteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return &pb.WriteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: locationResp.Status.Message,
				},
			}, nil
		}

		locations = append(
			locations,
			locationResp.Location,
		)
	}

	return &pb.WriteFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "write locations allocated successfully",
		},
		Locations: locations,
	}, nil
}

func (m *MasterServer) AppendFile(
	ctx context.Context,
	req *pb.AppendFileRequest,
) (*pb.AppendFileResponse, error) {

	appendOffset, chunkIDs, err := m.metadata.AppendFile(
		req.Path,
		req.Length,
	)

	if err != nil {
		return &pb.AppendFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	locations := make(
		[]*pb.ChunkLocation,
		0,
		len(chunkIDs),
	)
	for _, chunkID := range chunkIDs {

		chunk, err :=
			m.metadata.GetChunkMetadata(chunkID)

		if err != nil {
			// return error response
		}

		// If this chunk has no primary,
		// choose one now.
		if chunk.Primary == nil {

			server, err :=
				m.metadata.AllocateChunkServer(3)

			if err != nil {
				// return error response
			}

			primary := server[0]
			replica1 := server[1]
			replica2 := server[2]

			err = m.metadata.SetChunkPrimary(
				chunkID,
				primary,
			)

			if err != nil {
				return &pb.AppendFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}
			err = m.metadata.SetChunkReplica(
				chunkID,
				replica1,
			)

			if err != nil {
				return &pb.AppendFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}

			err = m.metadata.SetChunkReplica(
				chunkID,
				replica2,
			)

			if err != nil {
				return &pb.AppendFileResponse{
					Status: &pb.Status{
						Success: false,
						Message: err.Error(),
					},
				}, nil
			}
		}
		locationResp, err :=
			m.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: &pb.ChunkHandle{
						Id:   chunkID,
						Path: req.Path,
					},
				},
			)

		if err != nil {
			return &pb.AppendFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return &pb.AppendFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: locationResp.Status.Message,
				},
			}, nil
		}

		locations = append(
			locations,
			locationResp.Location,
		)
	}
	return &pb.AppendFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "append locations allocated successfully",
		},
		Offset:    appendOffset,
		Locations: locations,
	}, nil
}

func (m *MasterServer) RangeDeleteFile(
	ctx context.Context,
	req *pb.RangeDeleteFileRequest,
) (*pb.RangeDeleteFileResponse, error) {
	before_range, in_range, after_range, startOffset_startChunk, endOffset_endChunk, err := m.metadata.RangeDeleteFile(
		req.Path,
		req.Offset,
		req.Length,
	)

	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "file not found",
				},
			}, nil
		}

		return &pb.RangeDeleteFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	before_range_locations := make(
		[]*pb.ChunkLocation,
		0,
		len(before_range),
	)

	range_locations := make(
		[]*pb.ChunkLocation,
		0,
		len(in_range),
	)

	after_range_locations := make(
		[]*pb.ChunkLocation,
		0,
		len(after_range),
	)

	for _, chunkID := range before_range {

		locationResp, err := m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   chunkID,
					Path: req.Path,
				},
			},
		)

		if err != nil {
			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to get chunk location",
				},
			}, nil
		}

		before_range_locations = append(
			before_range_locations,
			locationResp.Location,
		)
	}

	for _, chunkID := range in_range {

		locationResp, err := m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   chunkID,
					Path: req.Path,
				},
			},
		)

		if err != nil {
			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to get chunk location",
				},
			}, nil
		}

		range_locations = append(
			range_locations,
			locationResp.Location,
		)
	}

	for _, chunkID := range after_range {

		locationResp, err := m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   chunkID,
					Path: req.Path,
				},
			},
		)

		if err != nil {
			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return &pb.RangeDeleteFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to get chunk location",
				},
			}, nil
		}

		after_range_locations = append(
			after_range_locations,
			locationResp.Location,
		)
	}

	return &pb.RangeDeleteFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "truncate locations allocated successfully",
		},
		BeforeRange:      before_range_locations,
		Range:            range_locations,
		AfterRange:       after_range_locations,
		StartOffsetRange: startOffset_startChunk,
		EndOffsetRange:   endOffset_endChunk,
	}, nil
}

func (m *MasterServer) UpdateMasterMetadata(
	ctx context.Context,
	req *pb.UpdateMasterMetadataRequest,
) (*pb.UpdateMasterMetadataResponse, error) {

	isDelete := req.Delete
	handle := ChunkHandle{
		Id:   req.Chunk.Id,
		path: req.Chunk.Path,
	}
	err := m.metadata.UpdateMasterMetadata(req.Path, req.Size, handle, isDelete)
	if err != nil {
		return &pb.UpdateMasterMetadataResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.UpdateMasterMetadataResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk updated successfully",
		},
	}, nil
}
func (m *MasterServer) TruncateFile(
	ctx context.Context,
	req *pb.TruncateFileRequest,
) (*pb.TruncateFileResponse, error) {

	deleteIDs, truncateID, truncateSize, err :=
		m.metadata.TruncateFile(
			req.Path,
			req.Size,
		)

	if err != nil {
		return &pb.TruncateFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	deleteLocations :=
		make([]*pb.ChunkLocation, 0, len(deleteIDs))

	// Get locations of chunks that need to be deleted.
	for _, id := range deleteIDs {

		resp, err := m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   id,
					Path: req.Path,
				},
			},
		)

		if err != nil {
			return &pb.TruncateFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if resp.Status == nil ||
			!resp.Status.Success ||
			resp.Location == nil {

			return &pb.TruncateFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to get delete chunk location",
				},
			}, nil
		}

		deleteLocations = append(
			deleteLocations,
			resp.Location,
		)
	}

	// ------------------------------------------------------------
	// Get location of final chunk that needs truncation.
	// ------------------------------------------------------------

	var truncateLocation *pb.ChunkLocation

	if truncateID != 0 {

		resp, err := m.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   truncateID,
					Path: req.Path,
				},
			},
		)

		if err != nil {
			return &pb.TruncateFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: err.Error(),
				},
			}, nil
		}

		if resp.Status == nil ||
			!resp.Status.Success ||
			resp.Location == nil {

			return &pb.TruncateFileResponse{
				Status: &pb.Status{
					Success: false,
					Message: "failed to get truncate chunk location",
				},
			}, nil
		}

		truncateLocation = resp.Location
	}

	return &pb.TruncateFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "truncate plan created successfully",
		},
		DeleteChunks:      deleteLocations,
		TruncateChunk:     truncateLocation,
		TruncateChunkSize: truncateSize,
	}, nil
}

func (m *MasterServer) InsertFile(
	ctx context.Context,
	req *pb.InsertFileRequest,
) (*pb.InsertFileResponse, error) {

	if req == nil {
		return &pb.InsertFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: "request is nil",
			},
		}, nil
	}

	if req.Length == 0 {
		return &pb.InsertFileResponse{
			Status: &pb.Status{
				Success: true,
				Message: "nothing to insert",
			},
		}, nil
	}

	chunk, startOffset, err :=
		m.metadata.InsertFile(
			req.Path,
			req.Offset,
			req.Length,
		)

	if err != nil {
		return &pb.InsertFileResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}

	if chunk == 0 {
		return &pb.InsertFileResponse{
			Status: &pb.Status{
				Success: true,
				Message: "EOF insert",
			},
			StartChunk:  nil,
			StartOffset: 0,
		}, nil
	}

	location, err := m.GetChunkLocations(
		ctx,
		&pb.GetChunkLocationsRequest{
			ChunkHandle: &pb.ChunkHandle{
				Id:   chunk,
				Path: req.Path,
			},
		},
	)

	return &pb.InsertFileResponse{
		Status: &pb.Status{
			Success: true,
			Message: "insert plan generated",
		},
		StartChunk:  location.Location,
		StartOffset: startOffset,
	}, nil
}

func (m *MasterServer) RegisterChunkServer(
	ctx context.Context,
	req *pb.RegisterChunkServerRequest,
) (*pb.RegisterChunkServerResponse, error) {

	if req.Server == nil {
		return &pb.RegisterChunkServerResponse{
			Status: &pb.Status{
				Success: false,
				Message: "server info is missing",
			},
		}, nil
	}

	server := req.Server

	m.metadata.RegisterChunkServer(
		server.Id,
		server.Host,
		server.Port,
	)

	fmt.Printf(
		"ChunkServer registered: ID=%s %s:%d\n",
		server.Id,
		server.Host,
		server.Port,
	)

	return &pb.RegisterChunkServerResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk server registered",
		},
	}, nil
}

func chunkLocationFromMetadata(
	chunk *ChunkMetadata,
) *pb.ChunkLocation {

	location := &pb.ChunkLocation{
		Handle: &pb.ChunkHandle{
			Id:   chunk.Handle.Id,
			Path: chunk.Handle.path,
		},
	}

	if chunk.Primary != nil {
		location.Primary = &pb.ServerInfo{
			Id:   chunk.Primary.ID,
			Host: chunk.Primary.Host,
			Port: chunk.Primary.Port,
		}
	}

	for _, replica := range chunk.Replicas {

		location.Replicas =
			append(
				location.Replicas,
				&pb.ServerInfo{
					Id:   replica.ID,
					Host: replica.Host,
					Port: replica.Port,
				},
			)
	}

	return location
}

func (m *MasterServer) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {

	if req.Server == nil {
		return &pb.HeartbeatResponse{
			Status: &pb.Status{
				Success: false,
				Message: "server info is missing",
			},
		}, nil
	}

	m.metadata.UpdateHeartbeat(
		req.Server,
		req.Chunks,
		req.AvailableSpace,
	)

	fmt.Printf(
		"Heartbeat: %s, chunks=%d, available=%d\n",
		req.Server.Id,
		len(req.Chunks),
		req.AvailableSpace,
	)

	return &pb.HeartbeatResponse{
		Status: &pb.Status{
			Success: true,
			Message: "heartbeat received",
		},
	}, nil
}

func (m *MasterServer) StartFailureDetector(
	ctx context.Context,
) {

	ticker := time.NewTicker(
		5 * time.Second,
	)

	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():
			return

		case <-ticker.C:

			m.metadata.CheckChunkServers()
		}
	}
}

func (m *MasterServer) StartReplicationManager(
	ctx context.Context,
) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			m.replicationManager.RunOnce(ctx)

		case <-ctx.Done():
			return
		}
	}
}
