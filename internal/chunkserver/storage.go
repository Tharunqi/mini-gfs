package chunkserver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrChunkExists   = errors.New("chunk already exists")
	ErrChunkNotFound = errors.New("chunk not found")
)

type Chunk struct {
	Handle uint64
	Data   []byte
}

type Storage struct {
	mu      sync.RWMutex
	baseDir string

	// chunkID -> physical size
	chunks map[uint64]uint64
}

type ChunkHandle struct {
	Id   uint64
	path string
}

type ChunkServerInfo struct{
	ID string
	Address string
}

func NewStorage(baseDir string) *Storage {

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		panic(err)
	}

	s := &Storage{
		baseDir: baseDir,
		chunks:  make(map[uint64]uint64),
	}

	if err := s.loadChunks(); err != nil {
		panic(err)
	}

	return s
}

func (s *Storage) chunkPath(id uint64) string {
	return filepath.Join(
		s.baseDir,
		fmt.Sprintf("%d.chunk", id),
	)
}

// ============================================================
// WRITE CHUNK
// ============================================================

func (s *Storage) WriteChunk(
	handle ChunkHandle,
	offset uint64,
	data []byte,
) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.chunkPath(handle.Id)

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(
		int64(offset),
		io.SeekStart,
	)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		return err
	}

	s.chunks[handle.Id] = uint64(info.Size())

	return nil
}

// ============================================================
// READ CHUNK
// ============================================================

func (s *Storage) ReadChunk(
	handle ChunkHandle,
	offset uint64,
	length uint64,
) ([]byte, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.chunkPath(handle.Id)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrChunkNotFound
		}

		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())

	if offset >= size {
		return []byte{}, nil
	}

	available := size - offset

	if length > available {
		length = available
	}

	data := make([]byte, int(length))

	n, err := file.ReadAt(
		data,
		int64(offset),
	)

	if err != nil && err != io.EOF {
		return nil, err
	}

	return data[:n], nil
}

// ============================================================
// DELETE CHUNK
// ============================================================

func (s *Storage) DeleteChunk(
	handle ChunkHandle,
) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.chunkPath(handle.Id)

	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			delete(s.chunks, handle.Id)
			return ErrChunkNotFound
		}

		return err
	}

	delete(s.chunks, handle.Id)

	return nil
}

func (s *Storage) TruncateChunk(handle ChunkHandle, size uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.chunkPath(handle.Id)

	file, err := os.OpenFile(
		path,
		os.O_RDWR,
		0644,
	)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrChunkNotFound
		}

		return err
	}

	defer file.Close()

	if err := file.Truncate(int64(size)); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	s.chunks[handle.Id] = size

	return nil
}

func (s *Storage) GetChunkSize(handle ChunkHandle) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	size, exists := s.chunks[handle.Id]
	if !exists {
		return 0
	}

	return size
}

func (s *Storage) loadChunks() error {

	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if filepath.Ext(name) != ".chunk" {
			continue
		}

		// Example:
		// 17.chunk → 17
		idString := name[:len(name)-len(".chunk")]

		var id uint64

		if _, err := fmt.Sscanf(
			idString,
			"%d",
			&id,
		); err != nil {
			return fmt.Errorf(
				"invalid chunk filename %q: %w",
				name,
				err,
			)
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		s.chunks[id] = uint64(info.Size())
	}

	return nil
}
