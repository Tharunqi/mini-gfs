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
	chunks  map[uint64]uint64
}

type ChunkHandle struct {
	Id   uint64
	path string
}

func NewStorage() *Storage {
	os.MkdirAll("chunks", 0755)
	return &Storage{
		chunks:  make(map[uint64]uint64),
		baseDir: "chunks",
	}
}

func (s *Storage) WriteChunk(
	handle ChunkHandle,
	offset uint64,
	data []byte,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(
		s.baseDir,
		fmt.Sprintf("%d.chunk", handle.Id),
	)

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	s.chunks[handle.Id] = offset + uint64(len(data))

	return nil
}

func (s *Storage) ReadChunk(handle ChunkHandle, offset uint64, length uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := handle.path

	file, err := os.OpenFile(
		path,
		os.O_RDONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, length)
	file.Seek(int64(offset), 0)
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Storage) DeleteChunk(handle ChunkHandle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.chunks[handle.Id]; !exists {
		return ErrChunkNotFound
	}

	delete(s.chunks, handle.Id)
	return nil
}

func (s *Storage) Heartbeat() {
	// This function can be used to perform any periodic checks or maintenance tasks.
	// For now, it does nothing.
}
