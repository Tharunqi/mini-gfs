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
