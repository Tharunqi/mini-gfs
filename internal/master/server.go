package master

import (
	"context"
	"errors"

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
			Id: id,
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
	replicaLocations := make([]*pb.ServerInfo, 0, len(resp)-1)

	for i := 1; i < len(resp); i++ {
		replicaLocations = append(
			replicaLocations,
			&pb.ServerInfo{
				Id: resp[i],
			},
		)
	}

	primary := &pb.ServerInfo{
		Id: resp[0],
	}

	location := &pb.ChunkLocation{
		Handle: &pb.ChunkHandle{
			Id: req.ChunkHandle.Id,
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
	chunkHandle, err := m.metadata.AllocateChunk()
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
				Id: chunkHandle,
			},
		},
	}, nil
}
