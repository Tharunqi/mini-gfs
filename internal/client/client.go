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

	resp, err := c.masterClient.DeleteFile(
		ctx,
		&pb.DeleteFileRequest{
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

		return errors.New("delete failed")
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

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
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
			return err
		}

		if deleteResp.Status == nil ||
			!deleteResp.Status.Success {

			return fmt.Errorf(
				"failed deleting chunk %d: %s",
				location.Handle.Id,
				deleteResp.Status.Message,
			)
		}
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

		conn, chunkClient, err :=
			connectChunkServer(location)

		if err != nil {
			return err
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
			return err
		}

		if deleteResp.Status == nil ||
			!deleteResp.Status.Success {

			return fmt.Errorf(
				"failed deleting chunk %d",
				location.Handle.Id,
			)
		}
	}

	// Truncate final surviving chunk.
	if resp.TruncateChunk != nil {

		conn, chunkClient, err :=
			connectChunkServer(resp.TruncateChunk)

		if err != nil {
			return err
		}

		truncateResp, err :=
			chunkClient.TruncateChunk(
				ctx,
				&pb.TruncateChunkRequest{
					ChunkHandle: resp.TruncateChunk.Handle,
					Size:        resp.TruncateChunkSize,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if truncateResp.Status == nil ||
			!truncateResp.Status.Success {

			return fmt.Errorf(
				"failed truncating chunk %d",
				resp.TruncateChunk.Handle.Id,
			)
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

		conn, chunkClient, err :=
			connectChunkServer(locationResp.Location)

		if err != nil {
			return err
		}

		deleteResp, err :=
			chunkClient.DeleteChunk(
				ctx,
				&pb.DeleteChunkRequest{
					ChunkHandle: handle,
				},
			)

		conn.Close()

		if err != nil {
			return err
		}

		if deleteResp.Status == nil ||
			!deleteResp.Status.Success {

			return fmt.Errorf(
				"failed deleting chunk %d: %s",
				handle.Id,
				deleteResp.Status.Message,
			)
		}

		fmt.Printf(
			"[Client Insert] deleted chunk %d\n",
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
