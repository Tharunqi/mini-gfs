package master

import (
	"fmt"
	"math/rand"
)

type LeaseManager struct {
	metadata *MetadataStore
}

func NewLeaseManager(metadata *MetadataStore) *LeaseManager {
	return &LeaseManager{
		metadata: metadata,
	}
}

// CheckPrimaries checks every chunk and makes sure
// that its primary server is currently available.
//
// If the current primary is unavailable, one of the
// healthy replicas is randomly promoted to primary.
func (l *LeaseManager) CheckPrimaries(dead_servers map[string]*ChunkServerInfo) {

	chunks := make(map[uint64]*ChunkMetadata)
	for _, server := range dead_servers {
		dead_chunks := server.Chunks
		for _, chunkID := range dead_chunks {
			chunk, exists := l.metadata.GetChunkMetadata(chunkID)

			if exists == nil {
				chunks[chunk.Handle.Id] = chunk
			}
		}
	}

	for _, chunk := range chunks {

		if chunk == nil {
			continue
		}

		// -------------------------------------------------
		// Current primary exists and is available.
		// Nothing to do.
		// -------------------------------------------------

		if chunk.Primary != nil &&
			l.metadata.IsChunkServerAvailable(
				chunk.Primary.ID,
			) {

			continue
		}

		// -------------------------------------------------
		// Primary is missing or unavailable.
		// Try to promote a healthy replica.
		// -------------------------------------------------

		l.assignPrimary(chunk)
	}
}

// assignPrimary chooses one healthy replica and
// promotes it to primary.
func (l *LeaseManager) assignPrimary(
	chunk *ChunkMetadata,
) {

	healthyReplicas :=
		make([]*ChunkServerInfo, 0)

	// -------------------------------------------------
	// Find replicas that are currently available.
	// -------------------------------------------------

	for _, replica := range chunk.Replicas {

		if replica == nil {
			continue
		}

		if l.metadata.IsChunkServerAvailable(
			replica.ID,
		) {

			healthyReplicas =
				append(
					healthyReplicas,
					replica,
				)
		}
	}

	// -------------------------------------------------
	// No healthy replica exists.
	//
	// ReplicationManager will handle this situation.
	// We cannot make a server primary if it doesn't
	// have the chunk.
	// -------------------------------------------------

	if len(healthyReplicas) == 0 {

		fmt.Printf(
			"[LeaseManager] Chunk %d has no healthy replica to promote\n",
			chunk.Handle.Id,
		)

		return
	}

	// -------------------------------------------------
	// Randomly choose one healthy replica.
	// We will improve this later during load balancing.
	// -------------------------------------------------

	newPrimary :=
		healthyReplicas[rand.Intn(len(healthyReplicas))]

	// -------------------------------------------------
	// Update Master metadata.
	// -------------------------------------------------

	err :=
		l.metadata.SetChunkPrimary(
			chunk.Handle.Id,
			newPrimary,
		)

	if err != nil {

		fmt.Printf(
			"[LeaseManager] Failed to set primary for chunk %d: %v\n",
			chunk.Handle.Id,
			err,
		)

		return
	}

	fmt.Printf(
		"[LeaseManager] Chunk %d: promoted %s to primary\n",
		chunk.Handle.Id,
		newPrimary.ID,
	)
}
