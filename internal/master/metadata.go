package master

import (
	"errors"
	"sync"

	"github.com/Tharunqi/mini-gfs/internal/config"
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

		file.ChunkHandles = append(
			file.ChunkHandles,
			m.chunkid,
		)
	}

	// Update file size if necessary.
	if endOffset > file.SizeBytes {
		file.SizeBytes = endOffset
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

		file.ChunkHandles = append(
			file.ChunkHandles,
			m.chunkid,
		)
	}

	// For the prototype we can update the logical size here.
	file.SizeBytes = endOffset

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
	newSize uint64,
	chunkIDs []uint64,
) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return ErrFileNotFound
	}

	// Replace the old chunk list completely.
	file.ChunkHandles = append(
		[]uint64(nil),
		chunkIDs...,
	)

	file.SizeBytes = newSize

	return nil
}
