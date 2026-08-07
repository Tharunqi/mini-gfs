package master

import (
	"errors"
	"sync"
)

var (
	ErrFileExists   = errors.New("file already exists")
	ErrFileNotFound = errors.New("file not found")
)

type FileMetadata struct {
	Path         string
	SizeBytes    uint64
	ChunkHandles []uint64
}

type MetadataStore struct {
	mu    sync.RWMutex
	files map[string]*FileMetadata
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		files: make(map[string]*FileMetadata),
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
		SizeBytes:    0,
		ChunkHandles: []uint64{},
	}

	return filemetadata, nil
}
