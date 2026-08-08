package chunkserver

import (
	"errors"
	"os"
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
	mu     sync.RWMutex
	chunks map[uint64]*Chunk
}

type ChunkHandle struct {
	Id   uint64
	path string
}

func NewStorage() *Storage {
	return &Storage{
		chunks: make(map[uint64]*Chunk),
	}
}

func (s *Storage) WriteChunk(handle ChunkHandle, offset uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := handle.path

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), 0)

	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	s.chunks[handle.Id] = &Chunk{
		Handle: handle.Id,
		Data:   data,
	}

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

func (s *Storage) DeleteChunk(handle uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.chunks[handle]; !exists {
		return ErrChunkNotFound
	}

	delete(s.chunks, handle)
	return nil
}

func (s *Storage) Heartbeat() {
	// This function can be used to perform any periodic checks or maintenance tasks.
	// For now, it does nothing.
}
