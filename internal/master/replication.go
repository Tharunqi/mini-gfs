package master

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/Tharunqi/mini-gfs/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	TargetReplicas = 2
)

type ReplicationManager struct {
	metadata *MetadataStore
}

func NewReplicationManager(
	metadata *MetadataStore,
) *ReplicationManager {

	return &ReplicationManager{
		metadata: metadata,
	}
}

func connectChunkServer(
	server *ChunkServerInfo,
) (*grpc.ClientConn, pb.ChunkServiceClient, error) {

	if server == nil {
		return nil, nil, errors.New(
			"chunk server is nil",
		)
	}

	address := fmt.Sprintf(
		"%s:%d",
		server.Host,
		server.Port,
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

	return conn,
		pb.NewChunkServiceClient(conn),
		nil
}

func (r *ReplicationManager) healthyCopies(
	chunk *ChunkMetadata,
) []*ChunkServerInfo {

	servers := make(
		[]*ChunkServerInfo,
		0,
		3,
	)

	if chunk.Primary != nil &&
		r.metadata.IsChunkServerAvailable(
			chunk.Primary.ID,
		) {

		servers = append(
			servers,
			chunk.Primary,
		)
	}

	for _, replica := range chunk.Replicas {

		if replica == nil {
			continue
		}

		if r.metadata.IsChunkServerAvailable(
			replica.ID,
		) {

			servers = append(
				servers,
				replica,
			)
		}
	}

	return servers
}

func (r *ReplicationManager) chooseDestination(
	chunk *ChunkMetadata,
) *ChunkServerInfo {

	servers :=
		r.metadata.GetAvailableChunkServers()

	for _, server := range servers {

		alreadyHasChunk := false

		if chunk.Primary != nil &&
			chunk.Primary.ID == server.ID {

			alreadyHasChunk = true
		}

		for _, replica := range chunk.Replicas {

			if replica != nil &&
				replica.ID == server.ID {

				alreadyHasChunk = true
				break
			}
		}

		if !alreadyHasChunk {
			return server
		}
	}

	return nil
}

func (r *ReplicationManager) chooseSource(
	chunk *ChunkMetadata,
	healthy []*ChunkServerInfo,
) *ChunkServerInfo {

	for _, server := range healthy {

		if chunk.Primary == nil ||
			server.ID != chunk.Primary.ID {

			return server
		}
	}

	if len(healthy) > 0 {
		return healthy[0]
	}

	return nil
}

func (r *ReplicationManager) replicateChunk(
	ctx context.Context,
	chunk *ChunkMetadata,
	source *ChunkServerInfo,
	destination *ChunkServerInfo,
) error {

	conn, client, err :=
		connectChunkServer(source)

	if err != nil {
		return err
	}

	defer conn.Close()

	fmt.Printf(
		"[ReplicationManager] Replicating chunk %d: %s -> %s\n",
		chunk.Handle.Id,
		source.ID,
		destination.ID,
	)

	resp, err :=
		client.ReplicateChunk(
			ctx,
			&pb.ReplicateChunkRequest{
				ChunkHandle: &pb.ChunkHandle{
					Id:   chunk.Handle.Id,
					Path: chunk.Handle.path,
				},
				Destination: &pb.ServerInfo{
					Id:   destination.ID,
					Host: destination.Host,
					Port: destination.Port,
				},
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
			"replication failed",
		)
	}

	return nil
}

func (r *ReplicationManager) RepairChunk(
	ctx context.Context,
	chunk *ChunkMetadata,
) {

	healthy :=
		r.healthyCopies(chunk)

	targetCopies := 1 + TargetReplicas

	if len(healthy) >= targetCopies {
		return
	}

	destination :=
		r.chooseDestination(chunk)

	if destination == nil {
		fmt.Printf(
			"[ReplicationManager] Chunk %d is under-replicated, but no destination is available\n",
			chunk.Handle.Id,
		)

		return
	}

	source :=
		r.chooseSource(
			chunk,
			healthy,
		)

	if source == nil {
		fmt.Printf(
			"[ReplicationManager] Chunk %d has no healthy source\n",
			chunk.Handle.Id,
		)

		return
	}

	fmt.Printf(
		"[ReplicationManager] Chunk %d: %d healthy copies, repairing %s -> %s\n",
		chunk.Handle.Id,
		len(healthy),
		source.ID,
		destination.ID,
	)

	err :=
		r.replicateChunk(
			ctx,
			chunk,
			source,
			destination,
		)

	if err != nil {

		fmt.Printf(
			"[ReplicationManager] Failed to replicate chunk %d: %v\n",
			chunk.Handle.Id,
			err,
		)

		return
	}

	// Only update metadata after physical replication succeeded.
	err =
		r.metadata.SetChunkReplica(
			chunk.Handle.Id,
			destination,
		)

	if err != nil {

		fmt.Printf(
			"[ReplicationManager] Failed to update metadata for chunk %d: %v\n",
			chunk.Handle.Id,
			err,
		)

		return
	}

	fmt.Printf(
		"[ReplicationManager] Chunk %d successfully replicated to %s\n",
		chunk.Handle.Id,
		destination.ID,
	)
}

func (r *ReplicationManager) CheckReplication(
	ctx context.Context,
) {

	chunks :=
		r.metadata.GetAllChunkMetadata()

	for _, chunk := range chunks {

		if chunk == nil {
			continue
		}

		r.RepairChunk(
			ctx,
			chunk,
		)
	}
}

func (r *ReplicationManager) RunOnce(
	ctx context.Context,
) {
	// repair chunks affected by those failures.
	r.CheckReplication(ctx)
}
