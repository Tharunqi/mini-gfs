package master

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Tharunqi/mini-gfs/internal/config"
	pb "github.com/Tharunqi/mini-gfs/internal/pb"
)

var (
	ErrFileExists    = errors.New("file already exists")
	ErrFileNotFound  = errors.New("file not found")
	ErrChunkNotFound = errors.New("chunk not found")
)

type ChunkHandle struct {
	Id   uint64
	path string
}

type ChunkServerInfo struct {
	ID   string
	Host string
	Port uint32

	LastHeartbeat  int64
	Chunks         []uint64
	AvailableSpace uint64
}

type ChunkMetadata struct {
	Handle   ChunkHandle
	Primary  *ChunkServerInfo
	Replicas []*ChunkServerInfo
}

type FileMetadata struct {
	Path         string
	SizeBytes    uint64
	ChunkHandles []uint64
}

type persistentMetadata struct {
	Files          map[string]*FileMetadata    `json:"files"`
	ChunkID        uint64                      `json:"chunk_id"`
	ChunkServers   map[string]*ChunkServerInfo `json:"chunk_servers"`
	ChunkLocations map[uint64]*ChunkMetadata   `json:"chunk_locations"`
}

type MetadataStore struct {
	mu              sync.RWMutex
	files           map[string]*FileMetadata
	chunkid         uint64
	dataPath        string
	allChunkServers map[string]*ChunkServerInfo
	chunkServers    map[string]*ChunkServerInfo
	chunkLocations  map[uint64]*ChunkMetadata
	nextServer      uint64
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		files:           make(map[string]*FileMetadata),
		chunkid:         1,
		dataPath:        "metadata.json",
		chunkServers:    make(map[string]*ChunkServerInfo),
		chunkLocations:  make(map[uint64]*ChunkMetadata),
		allChunkServers: make(map[string]*ChunkServerInfo),
		nextServer:      0,
	}
}

func (m *MetadataStore) CreateFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[path]; exists {
		return ErrFileExists
	}

	m.files[path] = &FileMetadata{
		Path:         path,
		SizeBytes:    0,
		ChunkHandles: []uint64{},
	}
	return nil
}

func (m *MetadataStore) DeleteFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[path]; !exists {
		return ErrFileNotFound
	}

	delete(m.files, path)
	return nil
}

// return status,size,chunkhandles
func (m *MetadataStore) OpenFile(path string) (*FileMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.files[path]
	if !exists {
		return nil, ErrFileNotFound
	}
	filemetadata := &FileMetadata{
		Path:         path,
		SizeBytes:    m.files[path].SizeBytes,
		ChunkHandles: m.files[path].ChunkHandles,
	}
	return filemetadata, nil
}

func (m *MetadataStore) AllocateChunk(path string, index uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return 0, ErrFileNotFound
	}

	if index > uint64(len(file.ChunkHandles)) {
		return 0, fmt.Errorf(
			"chunk index %d out of range (chunk count %d)",
			index,
			len(file.ChunkHandles),
		)
	}

	m.chunkid++

	newHandle := m.chunkid

	m.chunkLocations[newHandle] = &ChunkMetadata{
		Handle: ChunkHandle{
			Id:   newHandle,
			path: path,
		},
		Primary:  nil,
		Replicas: []*ChunkServerInfo{},
	}

	// Insert at index.
	file.ChunkHandles = append(
		file.ChunkHandles,
		0,
	)

	copy(
		file.ChunkHandles[index+1:],
		file.ChunkHandles[index:],
	)

	file.ChunkHandles[index] = newHandle

	return newHandle, nil
}

func (m *MetadataStore) WriteFile(path string, offset uint64, length uint64) ([]uint64, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return nil, ErrFileNotFound
	}

	// Nothing to write.
	if length == 0 {
		return []uint64{}, nil
	}

	// Determine first and last affected chunk.
	startChunk := offset / config.ChunkSize
	endOffset := offset + length
	endChunk := (endOffset - 1) / config.ChunkSize

	// Allocate enough chunks.
	for uint64(len(file.ChunkHandles)) <= endChunk {
		// Don't call AllocateChunk() here because it also
		// acquires the mutex and modifies ChunkHandles.

		m.chunkid++

		newChunkID := m.chunkid

		file.ChunkHandles = append(
			file.ChunkHandles,
			newChunkID,
		)

		m.chunkLocations[newChunkID] = &ChunkMetadata{
			Handle: ChunkHandle{
				Id:   newChunkID,
				path: path,
			},
			Primary:  nil,
			Replicas: []*ChunkServerInfo{},
		}
	}

	// Return all chunks affected by this write.
	chunkHandles := make(
		[]uint64,
		endChunk-startChunk+1,
	)

	copy(
		chunkHandles,
		file.ChunkHandles[startChunk:endChunk+1],
	)

	return chunkHandles, nil
}

func (m *MetadataStore) AppendFile(
	path string,
	length uint64,
) (uint64, []uint64, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return 0, nil, ErrFileNotFound
	}

	if length == 0 {
		return file.SizeBytes, []uint64{}, nil
	}

	// Append starts at the current end of the file.
	appendOffset := file.SizeBytes

	startChunk := appendOffset / config.ChunkSize

	endOffset := appendOffset + length
	endChunk := (endOffset - 1) / config.ChunkSize

	// Allocate all chunks required by the append.
	for uint64(len(file.ChunkHandles)) <= endChunk {
		m.chunkid++

		newChunkID := m.chunkid

		file.ChunkHandles = append(
			file.ChunkHandles,
			newChunkID,
		)

		m.chunkLocations[newChunkID] = &ChunkMetadata{
			Handle: ChunkHandle{
				Id:   newChunkID,
				path: path,
			},
			Primary:  nil,
			Replicas: []*ChunkServerInfo{},
		}
	}

	// Return the affected chunks.
	chunkHandles := make(
		[]uint64,
		endChunk-startChunk+1,
	)

	copy(
		chunkHandles,
		file.ChunkHandles[startChunk:endChunk+1],
	)

	return appendOffset, chunkHandles, nil
}

func (m *MetadataStore) RangeDeleteFile(path string, offset uint64, length uint64) ([]uint64, []uint64, []uint64, uint64, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return nil, nil, nil, 0, 0, ErrFileNotFound
	}

	if length == 0 {
		return []uint64{}, []uint64{}, []uint64{}, 0, 0, nil
	}

	if offset > file.SizeBytes {
		return nil, nil, nil, 0, 0,
			errors.New("offset exceeds file size")
	}

	// If the requested deletion extends beyond EOF,
	// simply delete up to EOF.
	if length > file.SizeBytes-offset {
		length = file.SizeBytes - offset
	}

	startChunk := offset / config.ChunkSize
	startOffset_startChunk := offset % config.ChunkSize
	endOffset := offset + length
	endChunk := (endOffset - 1) / config.ChunkSize
	endOffset_endChunk := (endOffset - 1) % config.ChunkSize

	before_range := make(
		[]uint64,
		startChunk,
	)
	in_range := make(
		[]uint64,
		endChunk-startChunk+1,
	)
	after_range := make(
		[]uint64,
		len(file.ChunkHandles)-int(endChunk)-1,
	)

	copy(
		before_range,
		file.ChunkHandles[:startChunk],
	)
	copy(
		in_range,
		file.ChunkHandles[startChunk:endChunk+1],
	)

	copy(
		after_range,
		file.ChunkHandles[endChunk+1:],
	)

	return before_range, in_range, after_range, startOffset_startChunk, endOffset_endChunk, nil
}

func (m *MetadataStore) UpdateMasterMetadata(
	path string,
	size uint64,
	chunk ChunkHandle,
	isDelete uint64,
) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return ErrFileNotFound
	}

	if isDelete == 0 {
		// Remove the chunk from the file's chunk handles.
		for i, handle := range file.ChunkHandles {
			if handle == chunk.Id {
				file.ChunkHandles = append(
					file.ChunkHandles[:i],
					file.ChunkHandles[i+1:]...,
				)
				break
			}
		}
		delete(
			m.chunkLocations,
			chunk.Id,
		)
		file.SizeBytes = file.SizeBytes - size
	} else if isDelete == 1 {
		file.SizeBytes = file.SizeBytes - size
	} else {
		file.SizeBytes = file.SizeBytes + size
	}
	return nil
}

func (m *MetadataStore) TruncateFile(
	path string,
	size uint64,
) (
	deleteChunks []uint64,
	truncateChunk uint64,
	truncateChunkSize uint64,
	err error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return nil, 0, 0, ErrFileNotFound
	}

	// Cannot extend using truncate.
	if size > file.SizeBytes {
		return nil, 0, 0, errors.New(
			"truncate size exceeds file size",
		)
	}

	// Nothing to do.
	if size == file.SizeBytes {
		return []uint64{}, 0, 0, nil
	}

	// ------------------------------------------------------------
	// TRUNCATE TO ZERO
	// ------------------------------------------------------------

	if size == 0 {

		deleteChunks = make(
			[]uint64,
			len(file.ChunkHandles),
		)

		copy(
			deleteChunks,
			file.ChunkHandles,
		)

		return deleteChunks, 0, 0, nil
	}

	// ------------------------------------------------------------
	// FIND FINAL SURVIVING CHUNK
	// ------------------------------------------------------------

	chunkSize := uint64(config.ChunkSize)

	// Index of the chunk containing the final surviving byte.
	truncateChunkIndex :=
		(size - 1) / chunkSize

	// Number of bytes that should remain in that chunk.
	truncateChunkSize = size % chunkSize

	// If size falls exactly on a chunk boundary,
	// the final surviving chunk is completely full.
	if truncateChunkSize == 0 {
		truncateChunkSize = chunkSize
	}

	// Safety check.
	if truncateChunkIndex >= uint64(len(file.ChunkHandles)) {
		return nil, 0, 0, fmt.Errorf(
			"truncate chunk index %d out of range",
			truncateChunkIndex,
		)
	}

	truncateChunk =
		file.ChunkHandles[truncateChunkIndex]

	// ------------------------------------------------------------
	// CHUNKS AFTER THE FINAL SURVIVING CHUNK
	// ------------------------------------------------------------

	deleteStart :=
		int(truncateChunkIndex) + 1

	if deleteStart < len(file.ChunkHandles) {

		deleteChunks = append(
			deleteChunks,
			file.ChunkHandles[deleteStart:]...,
		)
	}

	return deleteChunks, truncateChunk, truncateChunkSize, nil
}

func (m *MetadataStore) InsertFile(
	path string,
	offset uint64,
	length uint64,
) (uint64, uint64, error) {

	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[path]
	if !exists {
		return 0, 0, ErrFileNotFound
	}

	if offset > file.SizeBytes {
		return 0, 0, errors.New(
			"offset exceeds file size",
		)
	}

	if length == 0 {
		return 0, 0, nil
	}

	if offset == file.SizeBytes {
		return 0, 0, nil
	}

	startChunk := offset / config.ChunkSize
	startOffset := offset % config.ChunkSize

	return file.ChunkHandles[startChunk], startOffset, nil
}

func (m *MetadataStore) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data := persistentMetadata{
		Files:          m.files,
		ChunkID:        m.chunkid,
		ChunkServers:   m.chunkServers,
		ChunkLocations: m.chunkLocations,
	}

	bytes, err := json.MarshalIndent(
		data,
		"",
		"  ",
	)
	if err != nil {
		return err
	}

	// Write to a temporary file first.
	tmpPath := m.dataPath + ".tmp"

	err = os.WriteFile(
		tmpPath,
		bytes,
		0644,
	)
	if err != nil {
		return err
	}

	// Replace the old metadata file.
	err = os.Rename(
		tmpPath,
		m.dataPath,
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *MetadataStore) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bytes, err := os.ReadFile(
		m.dataPath,
	)

	if err != nil {
		if os.IsNotExist(err) {
			// First startup.
			m.files = make(
				map[string]*FileMetadata,
			)

			m.chunkid = 1

			m.chunkServers = make(
				map[string]*ChunkServerInfo,
			)

			m.chunkLocations = make(
				map[uint64]*ChunkMetadata,
			)

			return nil
		}

		return err
	}

	var data persistentMetadata

	err = json.Unmarshal(
		bytes,
		&data,
	)
	if err != nil {
		return err
	}

	if data.Files == nil {
		data.Files = make(
			map[string]*FileMetadata,
		)
	}

	if data.ChunkServers == nil {
		data.ChunkServers = make(
			map[string]*ChunkServerInfo,
		)
	}

	if data.ChunkLocations == nil {
		data.ChunkLocations = make(
			map[uint64]*ChunkMetadata,
		)
	}

	m.files = data.Files
	m.chunkid = data.ChunkID
	m.chunkServers = data.ChunkServers
	m.chunkLocations = data.ChunkLocations

	return nil
}

func (m *MetadataStore) RegisterChunkServer(
	id string,
	host string,
	port uint32,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	server := &ChunkServerInfo{
		ID:   id,
		Host: host,
		Port: port,

		LastHeartbeat: time.Now().Unix(),
		Chunks:        []uint64{},

		AvailableSpace: 0,
	}

	// Remember this server permanently.
	m.allChunkServers[id] = server

	// Mark it currently available.
	m.chunkServers[id] = server
}

func (m *MetadataStore) GetChunkMetadata(
	chunkID uint64,
) (*ChunkMetadata, error) {

	m.mu.RLock()
	defer m.mu.RUnlock()

	chunk, exists := m.chunkLocations[chunkID]

	if !exists {
		return nil, errors.New(
			"chunk not found",
		)
	}

	return chunk, nil
}

func (m *MetadataStore) SetChunkPrimary(
	chunkID uint64,
	server *ChunkServerInfo,
) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	chunk, exists := m.chunkLocations[chunkID]

	if !exists {
		return errors.New("chunk not found")
	}

	chunk.Primary = server

	return nil
}

func (m *MetadataStore) AllocateChunkServer() (*ChunkServerInfo, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.chunkServers) == 0 {
		return nil, errors.New(
			"no chunk servers available",
		)
	}

	servers := make([]*ChunkServerInfo, 0, len(m.chunkServers))

	for _, server := range m.chunkServers {
		servers = append(servers, server)
	}

	server := servers[m.nextServer%uint64(len(servers))]

	m.nextServer++

	return server, nil
}

func (m *MetadataStore) DeleteChunkMetadata(
	chunkID uint64,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.chunkLocations, chunkID)
}

func (m *MetadataStore) UpdateHeartbeat(
	server *pb.ServerInfo,
	chunks []*pb.ChunkHandle,
	availableSpace uint64,
) {

	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists :=
		m.chunkServers[server.Id]

	if !exists {
		// Server may have restarted or Master may have
		// started after the server.
		info = &ChunkServerInfo{
			ID:   server.Id,
			Host: server.Host,
			Port: server.Port,
		}

		m.chunkServers[server.Id] = info
	}

	info.Host = server.Host
	info.Port = server.Port

	info.LastHeartbeat = time.Now().Unix()

	info.AvailableSpace = availableSpace

	info.Chunks = make(
		[]uint64,
		0,
		len(chunks),
	)

	for _, chunk := range chunks {
		info.Chunks = append(
			info.Chunks,
			chunk.Id,
		)
	}
}

func (m *MetadataStore) CheckChunkServers() {

	const heartbeatInterval = 5 * time.Second
	const missedHeartbeats = 3

	timeout :=
		heartbeatInterval * missedHeartbeats

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, server := range m.chunkServers {

		lastHeartbeat :=
			time.Unix(
				server.LastHeartbeat,
				0,
			)

		if time.Since(lastHeartbeat) > timeout {

			fmt.Printf(
				"ChunkServer %s missed 3 heartbeats. Marking unavailable.\n",
				id,
			)

			delete(
				m.chunkServers,
				id,
			)
		}
	}
}
func (m *MetadataStore) IsChunkServerAvailable(
	id string,
) bool {

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists :=
		m.chunkServers[id]

	return exists
}
