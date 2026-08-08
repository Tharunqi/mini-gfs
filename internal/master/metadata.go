package master

import (
	"errors"
	"sync"
)

var (
	ErrFileExists   = errors.New("file already exists")
	ErrFileNotFound = errors.New("file not found")
)

type ChunkHandle struct {
	Id   uint64
	path string
}

type FileMetadata struct {
	Path         string
	SizeBytes    uint64
	ChunkHandles []uint64
}

type MetadataStore struct {
	mu      sync.RWMutex
	files   map[string]*FileMetadata
	chunkid uint64
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		files:   make(map[string]*FileMetadata),
		chunkid: 1,
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

func (m *MetadataStore) GetChunkLocations(chunkHandle uint64) ([]string, error) {
	// For simplicity, we return a static list of chunk server addresses.
	// In a real implementation, this would query the metadata store for the actual locations.
	return []string{"localhost:50052"}, nil
}

func (m *MetadataStore) AllocateChunk(path string) (uint64, error) {
	m.chunkid++
	m.files[path].ChunkHandles = append(m.files[path].ChunkHandles, m.chunkid)

	return m.chunkid, nil
}

func (m *MetadataStore) UpdateChunkMetadata(handle ChunkHandle, offset uint64, bytesWritten uint64) error {

	end := offset + bytesWritten

	// Find the file containing this chunk.
	var file *FileMetadata
	file = m.files[handle.path]

	if file == nil {
		return ErrFileNotFound
	}
	// Then:
	if end > file.SizeBytes {
		file.SizeBytes = end
	}

	return nil
}
