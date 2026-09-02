package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tharunqi/mini-gfs/internal/config"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	masterConn   *grpc.ClientConn
	masterClient pb.MasterServiceClient
}

func New(masterAddress string) (*Client, error) {

	conn, err := grpc.Dial(
		masterAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		masterConn:   conn,
		masterClient: pb.NewMasterServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c.masterConn != nil {
		return c.masterConn.Close()
	}

	return nil
}

func connectChunkServer(
	location *pb.ChunkLocation,
) (*grpc.ClientConn, pb.ChunkServiceClient, error) {

	if location == nil || location.Primary == nil {
		return nil, nil, errors.New(
			"chunk location has no primary server",
		)
	}

	address := fmt.Sprintf(
		"%s:%d",
		location.Primary.Host,
		location.Primary.Port,
	)

	conn, err := grpc.Dial(
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

func (c *Client) Create(
	ctx context.Context,
	path string,
) error {

	resp, err := c.masterClient.CreateFile(
		ctx,
		&pb.CreateFileRequest{
			Path: path,
		},
	)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("create failed")
	}

	return nil
}

func (c *Client) Delete(
	ctx context.Context,
	path string,
) error {

	// First get the file metadata so that we know which chunks
	// physically belong to this file.
	openResp, err := c.masterClient.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	if openResp.Status == nil || !openResp.Status.Success {
		message := "failed to open file"
		if openResp.Status != nil {
			message = openResp.Status.Message
		}

		return errors.New(message)
	}

	// Delete every chunk from all of its physical copies.
	for _, handle := range openResp.Chunks {

		locationResp, err := c.masterClient.GetChunkLocations(
			ctx,
			&pb.GetChunkLocationsRequest{
				ChunkHandle: handle,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed to get location for chunk %d: %w",
				handle.Id,
				err,
			)
		}

		if locationResp.Status == nil || !locationResp.Status.Success {
			message := "failed to get chunk location"
			if locationResp.Status != nil {
				message = locationResp.Status.Message
			}

			return fmt.Errorf(
				"failed to get location for chunk %d: %s",
				handle.Id,
				message,
			)
		}

		err = c.deleteChunkFromAllReplicas(
			ctx,
			locationResp.Location,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to delete chunk %d: %w",
				handle.Id,
				err,
			)
		}
	}

	// All physical chunks have now been deleted.
	// Finally remove the file metadata from Master.
	deleteResp, err := c.masterClient.DeleteFile(
		ctx,
		&pb.DeleteFileRequest{
			Path: path,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}

	if deleteResp.Status == nil || !deleteResp.Status.Success {
		message := "failed to delete file metadata"
		if deleteResp.Status != nil {
			message = deleteResp.Status.Message
		}

		return errors.New(message)
	}

	return nil
}

type FileInfo struct {
	Path   string
	Size   uint64
	Chunks []*pb.ChunkHandle
}

func (c *Client) Open(
	ctx context.Context,
	path string,
) (*FileInfo, error) {

	resp, err := c.masterClient.OpenFile(
		ctx,
		&pb.OpenFileRequest{
			Path: path,
		},
	)

	if err != nil {
		return nil, err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return nil, errors.New(resp.Status.Message)
		}

		return nil, errors.New("open failed")
	}

	return &FileInfo{
		Path:   path,
		Size:   resp.SizeBytes,
		Chunks: resp.Chunks,
	}, nil
}

func (c *Client) Read(
	ctx context.Context,
	path string,
) ([]byte, error) {

	info, err := c.Open(ctx, path)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, info.Size)

	var bytesRead uint64

	for i, handle := range info.Chunks {

		remaining := info.Size - bytesRead

		if remaining == 0 {
			break
		}

		readLength := uint64(config.ChunkSize)

		if remaining < readLength {
			readLength = remaining
		}

		locationResp, err :=
			c.masterClient.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			return nil, err
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return nil, fmt.Errorf(
				"failed to get location for chunk %d",
				handle.Id,
			)
		}

		conn, chunkClient, err :=
			connectChunkServer(locationResp.Location)

		if err != nil {
			return nil, err
		}

		readResp, err :=
			chunkClient.ReadChunk(
				ctx,
				&pb.ReadChunkRequest{
					ChunkHandle: handle,
					Offset:      0,
					Length:      readLength,
				},
			)

		conn.Close()

		if err != nil {
			return nil, err
		}

		if readResp.Status == nil ||
			!readResp.Status.Success {

			return nil, fmt.Errorf(
				"failed reading chunk %d: %s",
				handle.Id,
				readResp.Status.Message,
			)
		}

		result = append(result, readResp.Data...)
		bytesRead += uint64(len(readResp.Data))

		_ = i
	}

	if uint64(len(result)) != info.Size {
		return nil, fmt.Errorf(
			"read size mismatch: metadata=%d actual=%d",
			info.Size,
			len(result),
		)
	}

	return result, nil
}

func (c *Client) Write(
	ctx context.Context,
	path string,
	offset uint64,
	data []byte,
) error {

	if len(data) == 0 {
		return nil
	}

	resp, err :=
		c.masterClient.WriteFile(
			ctx,
			&pb.WriteFileRequest{
				Path:   path,
				Offset: offset,
				Length: uint64(len(data)),
			},
		)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("write planning failed")
	}

	locations := resp.Locations

	chunkSize := uint64(config.ChunkSize)

	dataOffset := uint64(0)

	for _, location := range locations {

		if dataOffset >= uint64(len(data)) {
			break
		}

		chunkIndex := (offset + dataOffset) / chunkSize
		chunkOffset := (offset + dataOffset) % chunkSize

		_ = chunkIndex

		remaining := uint64(len(data)) - dataOffset

		writeLength := chunkSize - chunkOffset

		if remaining < writeLength {
			writeLength = remaining
		}

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
		}

		writeResp, err :=
			chunkClient.WriteChunk(
				ctx,
				&pb.WriteChunkRequest{
					ChunkHandle: location.Handle,
					Offset:      chunkOffset,
					Data:        data[dataOffset : dataOffset+writeLength],
					Replicas:    location.Replicas,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if writeResp.Status == nil ||
			!writeResp.Status.Success {

			return fmt.Errorf(
				"write to chunk %d failed: %s",
				location.Handle.Id,
				writeResp.Status.Message,
			)
		}

		dataOffset += writeLength
	}

	return nil
}

func (c *Client) Append(
	ctx context.Context,
	path string,
	data []byte,
) error {

	if len(data) == 0 {
		return nil
	}

	resp, err :=
		c.masterClient.AppendFile(
			ctx,
			&pb.AppendFileRequest{
				Path:   path,
				Length: uint64(len(data)),
			},
		)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("append planning failed")
	}

	chunkSize := uint64(config.ChunkSize)
	dataOffset := uint64(0)

	for _, location := range resp.Locations {

		absoluteOffset :=
			resp.Offset + dataOffset

		chunkOffset :=
			absoluteOffset % chunkSize

		remaining :=
			uint64(len(data)) - dataOffset

		writeLength :=
			chunkSize - chunkOffset

		if remaining < writeLength {
			writeLength = remaining
		}

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
		}

		writeResp, err :=
			chunkClient.WriteChunk(
				ctx,
				&pb.WriteChunkRequest{
					ChunkHandle: location.Handle,
					Offset:      chunkOffset,
					Data:        data[dataOffset : dataOffset+writeLength],
					Replicas:    location.Replicas,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if writeResp.Status == nil ||
			!writeResp.Status.Success {

			return fmt.Errorf(
				"append to chunk %d failed: %s",
				location.Handle.Id,
				writeResp.Status.Message,
			)
		}
		dataOffset += writeLength
	}

	return nil
}

func (c *Client) DeleteRange(
	ctx context.Context,
	path string,
	offset uint64,
	length uint64,
) error {

	if length == 0 {
		return nil
	}

	resp, err :=
		c.masterClient.RangeDeleteFile(
			ctx,
			&pb.RangeDeleteFileRequest{
				Path:   path,
				Offset: offset,
				Length: length,
			},
		)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("range delete planning failed")
	}

	affected := resp.Range

	if len(affected) == 0 {
		return nil
	}

	// --------------------------------------------------------
	// Build the replacement buffer.
	//
	// Only the affected region is rebuilt:
	//
	// prefix + suffix
	//
	// Chunks in AfterRange remain untouched.
	// --------------------------------------------------------

	buffer := make([]byte, 0)

	first := affected[0]

	// Prefix before deletion.
	if resp.StartOffsetRange > 0 {

		conn, chunkClient, err :=
			connectChunkServer(first)

		if err != nil {
			return err
		}

		readResp, err :=
			chunkClient.ReadChunk(
				ctx,
				&pb.ReadChunkRequest{
					ChunkHandle: first.Handle,
					Offset:      0,
					Length:      resp.StartOffsetRange,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if !readResp.Status.Success {
			return errors.New(readResp.Status.Message)
		}

		buffer = append(buffer, readResp.Data...)
	}

	// --------------------------------------------------------
	// Suffix after deletion.
	// --------------------------------------------------------

	last := affected[len(affected)-1]

	suffixOffset := resp.EndOffsetRange + 1

	conn, chunkClient, err :=
		connectChunkServer(last)

	if err != nil {
		return err
	}

	readResp, err :=
		chunkClient.ReadChunk(
			ctx,
			&pb.ReadChunkRequest{
				ChunkHandle: last.Handle,
				Offset:      suffixOffset,
				Length:      uint64(config.ChunkSize),
			},
		)

	conn.Close()

	if err != nil {
		return err
	}

	if !readResp.Status.Success {
		return errors.New(readResp.Status.Message)
	}

	buffer = append(buffer, readResp.Data...)

	// --------------------------------------------------------
	// Delete the old affected chunks.
	// DeleteChunk already updates Master metadata.
	// --------------------------------------------------------

	for _, location := range affected {

		err := c.deleteChunkFromAllReplicas(
			ctx,
			location,
		)

		if err != nil {
			return err
		}

		fmt.Printf(
			"[Client DeleteRange] deleted chunk %d from all available replicas\n",
			location.Handle.Id,
		)
	}
	// --------------------------------------------------------
	// Number of chunks required for rebuilt buffer.
	// --------------------------------------------------------

	newChunkCount := 0

	if len(buffer) > 0 {
		newChunkCount =
			(len(buffer) + int(config.ChunkSize) - 1) /
				int(config.ChunkSize)
	}

	// --------------------------------------------------------
	// Allocate replacement chunks at the same logical position.
	// --------------------------------------------------------

	insertIndex := uint64(len(resp.BeforeRange))

	newLocations := make(
		[]*pb.ChunkLocation,
		0,
		newChunkCount,
	)

	for i := 0; i < newChunkCount; i++ {

		allocateResp, err :=
			c.masterClient.AllocateChunk(
				ctx,
				&pb.AllocateChunkRequest{
					Path:  path,
					Index: insertIndex + uint64(i),
				},
			)

		if err != nil {
			return err
		}

		if allocateResp.Status == nil ||
			!allocateResp.Status.Success {

			return fmt.Errorf(
				"failed to allocate replacement chunk",
			)
		}

		newLocations = append(
			newLocations,
			allocateResp.Location,
		)
	}

	// --------------------------------------------------------
	// Write rebuilt data.
	// --------------------------------------------------------

	dataOffset := 0

	for _, location := range newLocations {

		end := dataOffset + int(config.ChunkSize)

		if end > len(buffer) {
			end = len(buffer)
		}

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
		}

		writeResp, err :=
			chunkClient.WriteChunk(
				ctx,
				&pb.WriteChunkRequest{
					ChunkHandle: location.Handle,
					Offset:      0,
					Data:        buffer[dataOffset:end],
					Replicas:    location.Replicas,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if writeResp.Status == nil ||
			!writeResp.Status.Success {

			return fmt.Errorf(
				"failed writing replacement chunk %d",
				location.Handle.Id,
			)
		}

		dataOffset = end
	}

	return nil
}

func (c *Client) Truncate(
	ctx context.Context,
	path string,
	size uint64,
) error {

	resp, err :=
		c.masterClient.TruncateFile(
			ctx,
			&pb.TruncateFileRequest{
				Path: path,
				Size: size,
			},
		)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("truncate planning failed")
	}

	// Delete chunks after the final surviving chunk.
	for _, location := range resp.DeleteChunks {

		err := c.deleteChunkFromAllReplicas(
			ctx,
			location,
		)

		if err != nil {
			return err
		}

		fmt.Printf(
			"[Client Truncate] deleted chunk %d from all available replicas\n",
			location.Handle.Id,
		)
	}

	// Truncate final surviving chunk.
	fmt.Printf(
		"[Client Truncate] requested file size=%d, chunk=%d, chunk truncate size=%d\n",
		size,
		resp.TruncateChunk.Handle.Id,
		resp.TruncateChunkSize,
	)
	if resp.TruncateChunk != nil {

		err := c.truncateChunkFromAllReplicas(
			ctx,
			resp.TruncateChunk,
			resp.TruncateChunkSize,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *Client) Insert(
	ctx context.Context,
	path string,
	offset uint64,
	data []byte,
) error {

	if len(data) == 0 {
		return nil
	}

	// --------------------------------------------------------
	// 1. Ask Master where the insertion starts.
	// --------------------------------------------------------

	resp, err := c.masterClient.InsertFile(
		ctx,
		&pb.InsertFileRequest{
			Path:   path,
			Offset: offset,
			Length: uint64(len(data)),
		},
	)

	if err != nil {
		return err
	}

	if resp.Status == nil || !resp.Status.Success {
		if resp.Status != nil {
			return errors.New(resp.Status.Message)
		}

		return errors.New("insert planning failed")
	}

	// --------------------------------------------------------
	// 2. Inserting at EOF.
	//
	// Master returns no start chunk.
	// Just use Append.
	// --------------------------------------------------------

	if resp.StartChunk == nil {
		fmt.Println("[Client Insert] insertion at EOF → using Append")

		err := c.Append(ctx, path, data)

		if err != nil {
			return fmt.Errorf(
				"EOF insert → append failed: %w",
				err,
			)
		}

		return nil
	}

	startChunk := resp.StartChunk
	startOffset := resp.StartOffset

	// --------------------------------------------------------
	// 3. Get complete file metadata.
	//
	// We need the ordered list of chunks so we can read
	// everything from startChunk to EOF.
	// --------------------------------------------------------

	info, err := c.Open(ctx, path)
	if err != nil {
		return err
	}

	startIndex := -1

	for i, handle := range info.Chunks {
		if handle.Id == startChunk.Handle.Id {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return fmt.Errorf(
			"start chunk %d not found in metadata",
			startChunk.Handle.Id,
		)
	}

	// --------------------------------------------------------
	// 4. Build the buffer.
	//
	// Keep:
	//
	//     prefix before insertion point
	//
	// then:
	//
	//     inserted data
	//
	// then:
	//
	//     EVERYTHING after insertion point until EOF
	// --------------------------------------------------------

	buffer := make([]byte, 0)

	// --------------------------------------------------------
	// Read start chunk.
	// --------------------------------------------------------

	conn, chunkClient, err :=
		connectChunkServer(startChunk)

	if err != nil {
		return err
	}

	readResp, err :=
		chunkClient.ReadChunk(
			ctx,
			&pb.ReadChunkRequest{
				ChunkHandle: startChunk.Handle,
				Offset:      0,
				Length:      uint64(config.ChunkSize),
			},
		)

	conn.Close()

	if err != nil {
		return err
	}

	if readResp.Status == nil ||
		!readResp.Status.Success {

		return fmt.Errorf(
			"failed reading chunk %d: %s",
			startChunk.Handle.Id,
			readResp.Status.Message,
		)
	}

	startData := readResp.Data

	if startOffset > uint64(len(startData)) {
		return fmt.Errorf(
			"start offset %d exceeds chunk %d size %d",
			startOffset,
			startChunk.Handle.Id,
			len(startData),
		)
	}

	// Prefix.
	buffer = append(
		buffer,
		startData[:startOffset]...,
	)

	// Inserted data.
	buffer = append(
		buffer,
		data...,
	)

	// Suffix of start chunk.
	buffer = append(
		buffer,
		startData[startOffset:]...,
	)

	// --------------------------------------------------------
	// Read every chunk AFTER the start chunk.
	// --------------------------------------------------------

	for i := startIndex + 1; i < len(info.Chunks); i++ {

		handle := info.Chunks[i]

		locationResp, err :=
			c.masterClient.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			return err
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return fmt.Errorf(
				"failed to get location for chunk %d",
				handle.Id,
			)
		}

		conn, chunkClient, err :=
			connectChunkServer(locationResp.Location)

		if err != nil {
			return err
		}

		readResp, err :=
			chunkClient.ReadChunk(
				ctx,
				&pb.ReadChunkRequest{
					ChunkHandle: handle,
					Offset:      0,
					Length:      uint64(config.ChunkSize),
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if readResp.Status == nil ||
			!readResp.Status.Success {

			return fmt.Errorf(
				"failed reading chunk %d: %s",
				handle.Id,
				readResp.Status.Message,
			)
		}

		buffer = append(
			buffer,
			readResp.Data...,
		)
	}

	fmt.Printf(
		"[Client Insert] rebuilt buffer: %d bytes\n",
		len(buffer),
	)

	// --------------------------------------------------------
	// 5. Delete every old chunk from startChunk → EOF.
	// --------------------------------------------------------

	for i := startIndex; i < len(info.Chunks); i++ {

		handle := info.Chunks[i]

		locationResp, err :=
			c.masterClient.GetChunkLocations(
				ctx,
				&pb.GetChunkLocationsRequest{
					ChunkHandle: handle,
				},
			)

		if err != nil {
			return err
		}

		if locationResp.Status == nil ||
			!locationResp.Status.Success {

			return fmt.Errorf(
				"failed to get location for chunk %d",
				handle.Id,
			)
		}

		err = c.deleteChunkFromAllReplicas(
			ctx,
			locationResp.Location,
		)

		if err != nil {
			return err
		}

		fmt.Printf(
			"[Client Insert] deleted chunk %d from all available replicas\n",
			handle.Id,
		)
	}

	// --------------------------------------------------------
	// 6. Calculate new number of chunks.
	// --------------------------------------------------------

	newChunkCount := 0

	if len(buffer) > 0 {
		newChunkCount =
			(len(buffer) + int(config.ChunkSize) - 1) /
				int(config.ChunkSize)
	}

	// --------------------------------------------------------
	// 7. Allocate replacement chunks.
	//
	// Start at the original logical position.
	// --------------------------------------------------------

	newLocations := make(
		[]*pb.ChunkLocation,
		0,
		newChunkCount,
	)

	for i := 0; i < newChunkCount; i++ {

		allocateResp, err :=
			c.masterClient.AllocateChunk(
				ctx,
				&pb.AllocateChunkRequest{
					Path:  path,
					Index: uint64(startIndex + i),
				},
			)

		if err != nil {
			return err
		}

		if allocateResp.Status == nil ||
			!allocateResp.Status.Success {

			return fmt.Errorf(
				"failed allocating replacement chunk",
			)
		}

		newLocations = append(
			newLocations,
			allocateResp.Location,
		)

		fmt.Printf(
			"[Client Insert] allocated chunk %d at logical index %d\n",
			allocateResp.Location.Handle.Id,
			startIndex+i,
		)
	}

	// --------------------------------------------------------
	// 8. Write rebuilt buffer.
	// --------------------------------------------------------

	dataOffset := 0

	for _, location := range newLocations {

		end := dataOffset + int(config.ChunkSize)

		if end > len(buffer) {
			end = len(buffer)
		}

		chunkData := buffer[dataOffset:end]

		fmt.Printf(
			"[Client Insert] writing %d bytes → chunk %d\n",
			len(chunkData),
			location.Handle.Id,
		)

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
		}

		writeResp, err :=
			chunkClient.WriteChunk(
				ctx,
				&pb.WriteChunkRequest{
					ChunkHandle: location.Handle,
					Offset:      0,
					Data:        chunkData,
					Replicas:    location.Replicas,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if writeResp.Status == nil ||
			!writeResp.Status.Success {

			return fmt.Errorf(
				"failed writing replacement chunk %d: %s",
				location.Handle.Id,
				writeResp.Status.Message,
			)
		}

		dataOffset = end
	}

	fmt.Println("[Client Insert] insert completed successfully")

	return nil
}

func (c *Client) deleteChunkFromAllReplicas(
	ctx context.Context,
	location *pb.ChunkLocation,
) error {

	if location == nil || location.Handle == nil {
		return errors.New("invalid chunk location")
	}

	// --------------------------------------------------------
	// 1. Delete all replicas first.
	//
	// DeleteReplicaChunk only removes the physical chunk.
	// It does NOT modify Master metadata.
	// --------------------------------------------------------

	for _, replica := range location.Replicas {

		if replica == nil {
			continue
		}

		conn, chunkClient, err := connectToServer(replica)

		if err != nil {
			fmt.Printf(
				"[Client] skipping unavailable replica %s while deleting chunk %d\n",
				replica.Id,
				location.Handle.Id,
			)
			continue
		}

		deleteResp, err :=
			chunkClient.DeleteReplicaChunk(
				ctx,
				&pb.DeleteReplicaChunkRequest{
					ChunkHandle: location.Handle,
				},
			)

		conn.Close()

		if err != nil {
			fmt.Printf(
				"[Client] failed deleting replica chunk %d from %s: %v\n",
				location.Handle.Id,
				replica.Id,
				err,
			)
			continue
		}

		if deleteResp.Status == nil ||
			!deleteResp.Status.Success {

			fmt.Printf(
				"[Client] failed deleting replica chunk %d from %s: %s\n",
				location.Handle.Id,
				replica.Id,
				deleteResp.Status.Message,
			)
			continue
		}

		fmt.Printf(
			"[Client] deleted replica chunk %d from %s\n",
			location.Handle.Id,
			replica.Id,
		)
	}

	// --------------------------------------------------------
	// 2. Delete the primary last.
	//
	// DeleteChunk performs:
	//   - physical deletion
	//   - Master metadata deletion
	// --------------------------------------------------------

	if location.Primary != nil {

		conn, chunkClient, err :=
			connectToServer(location.Primary)

		if err != nil {
			return fmt.Errorf(
				"failed connecting to primary %s while deleting chunk %d: %w",
				location.Primary.Id,
				location.Handle.Id,
				err,
			)
		}

		deleteResp, err :=
			chunkClient.DeleteChunk(
				ctx,
				&pb.DeleteChunkRequest{
					ChunkHandle: location.Handle,
				},
			)

		conn.Close()

		if err != nil {
			return fmt.Errorf(
				"failed deleting primary chunk %d: %w",
				location.Handle.Id,
				err,
			)
		}

		if deleteResp.Status == nil ||
			!deleteResp.Status.Success {

			message := "primary deletion failed"

			if deleteResp.Status != nil {
				message = deleteResp.Status.Message
			}

			return fmt.Errorf(
				"failed deleting primary chunk %d: %s",
				location.Handle.Id,
				message,
			)
		}

		fmt.Printf(
			"[Client] deleted primary chunk %d from %s\n",
			location.Handle.Id,
			location.Primary.Id,
		)
	}

	return nil
}

func connectToServer(
	server *pb.ServerInfo,
) (*grpc.ClientConn, pb.ChunkServiceClient, error) {

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

	client := pb.NewChunkServiceClient(conn)

	return conn, client, nil
}

func (c *Client) truncateChunkFromAllReplicas(
	ctx context.Context,
	location *pb.ChunkLocation,
	size uint64,
) error {

	if location == nil || location.Handle == nil {
		return errors.New("invalid chunk location")
	}

	// Truncate replicas first.
	for _, replica := range location.Replicas {

		if replica == nil {
			continue
		}

		conn, chunkClient, err := connectToServer(replica)

		if err != nil {
			fmt.Printf(
				"[Client] skipping unavailable replica %s while truncating chunk %d\n",
				replica.Id,
				location.Handle.Id,
			)
			continue
		}

		resp, err := chunkClient.TruncateChunk(
			ctx,
			&pb.TruncateChunkRequest{
				ChunkHandle: location.Handle,
				Size:        size,
				ReplicaOnly: true,
			},
		)

		conn.Close()

		if err != nil {
			fmt.Printf(
				"[Client] failed truncating chunk %d on replica %s: %v\n",
				location.Handle.Id,
				replica.Id,
				err,
			)
			continue
		}

		if resp.Status == nil || !resp.Status.Success {
			message := "replica truncation failed"

			if resp.Status != nil {
				message = resp.Status.Message
			}

			fmt.Printf(
				"[Client] failed truncating chunk %d on replica %s: %s\n",
				location.Handle.Id,
				replica.Id,
				message,
			)

			continue
		}

		fmt.Printf(
			"[Client] truncated chunk %d on replica %s\n",
			location.Handle.Id,
			replica.Id,
		)
	}

	// Finally truncate the primary.
	if location.Primary == nil {
		return errors.New("chunk has no primary")
	}

	conn, chunkClient, err := connectToServer(location.Primary)

	if err != nil {
		return fmt.Errorf(
			"failed connecting to primary %s while truncating chunk %d: %w",
			location.Primary.Id,
			location.Handle.Id,
			err,
		)
	}

	resp, err := chunkClient.TruncateChunk(
		ctx,
		&pb.TruncateChunkRequest{
			ChunkHandle: location.Handle,
			Size:        size,
			ReplicaOnly: false,
		},
	)

	conn.Close()

	if err != nil {
		return fmt.Errorf(
			"failed truncating primary chunk %d: %w",
			location.Handle.Id,
			err,
		)
	}

	if resp.Status == nil || !resp.Status.Success {
		message := "primary truncation failed"

		if resp.Status != nil {
			message = resp.Status.Message
		}

		return fmt.Errorf(
			"failed truncating primary chunk %d: %s",
			location.Handle.Id,
			message,
		)
	}

	fmt.Printf(
		"[Client] truncated primary chunk %d on %s\n",
		location.Handle.Id,
		location.Primary.Id,
	)

	return nil
}
