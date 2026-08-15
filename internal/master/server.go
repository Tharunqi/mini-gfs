package master

import (
	"context"
	"errors"

	"fmt"
	"net"
	"strconv"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

type MasterServer struct {
	pb.UnimplementedMasterServiceServer

	metadata *MetadataStore
}

func NewMasterServer() *MasterServer {
	return &MasterServer{
		metadata: NewMetadataStore(),
	}
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
	resp, err := m.metadata.GetChunkLocations(req.ChunkHandle.Id)
	if err != nil {
		return &pb.GetChunkLocationsResponse{
			Status: &pb.Status{
				Success: false,
				Message: err.Error(),
			},
		}, nil
	}
	host, portStr, err := net.SplitHostPort(resp[0])
	if err != nil {
		return nil, err
	}

	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return nil, err
	}
	primary := &pb.ServerInfo{
		Id:   "chunkserver-1",
		Host: host,
		Port: uint32(port),
	}
	replicaLocations := make([]*pb.ServerInfo, 0, len(resp)-1)

	for i := 1; i < len(resp); i++ {

		host, portStr, err := net.SplitHostPort(resp[i])
		if err != nil {
			return nil, err
		}

		port, err := strconv.ParseUint(portStr, 10, 32)
		if err != nil {
			return nil, err
		}

		replicaLocations = append(
			replicaLocations,
			&pb.ServerInfo{
				Id:   fmt.Sprintf("chunkserver-%d", i+1),
				Host: host,
				Port: uint32(port),
			},
		)
	}

	location := &pb.ChunkLocation{
		Handle: &pb.ChunkHandle{
			Id:   req.ChunkHandle.Id,
			Path: req.ChunkHandle.Path,
		},
		Primary:  primary,
		Replicas: replicaLocations,
	}

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

	return &pb.AllocateChunkResponse{
		Status: &pb.Status{
			Success: true,
			Message: "chunk allocated successfully",
		},
		Location: &pb.ChunkLocation{
			Handle: &pb.ChunkHandle{
				Id:   chunkHandle,
				Path: req.Path,
			},
			Primary: &pb.ServerInfo{
				Id:   "chunkserver1:50052",
				Host: "localhost",
				Port: 50052,
			},
		},
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
					Message: "failed to get chunk location",
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
					Message: "failed to get chunk location",
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
