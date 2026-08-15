package master

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Tharunqi/mini-gfs/internal/config"
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

		file.ChunkHandles = append(
			file.ChunkHandles,
			m.chunkid,
		)
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
