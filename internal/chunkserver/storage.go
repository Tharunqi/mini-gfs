package chunkserver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Tharunqi/mini-gfs/internal/config"
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

func NewStorage() *Storage {
	baseDir := "chunks"

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		panic(err)
	}

	return &Storage{
		baseDir: baseDir,
		chunks:  make(map[uint64]uint64),
	}
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
// READ WHOLE CHUNK
// Used internally by range deletion.
// ============================================================

func (s *Storage) readWholeChunk(id uint64) ([]byte, error) {

	path := s.chunkPath(id)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrChunkNotFound
		}

		return nil, err
	}

	return data, nil
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

// ============================================================
// TRUNCATE / RANGE DELETE
//
// Deletes:
//
//     [offset, offset + length)
//
// and shifts everything after the deleted range left.
//
// IMPORTANT:
// We do NOT read chunks before startChunk.
//
// We only read:
//
//     startChunk -> EOF
//
// because every chunk after the deletion point may need to
// shift left.
// ============================================================

func (s *Storage) RangeDeleteChunk(
	allHandles []ChunkHandle,
	offset uint64,
	length uint64,
) ([]uint64, uint64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if length == 0 {
		return []uint64{}, 0, nil
	}

	if len(allHandles) == 0 {
		return []uint64{}, 0, ErrChunkNotFound
	}

	// --------------------------------------------------------
	// Determine total file size from physical chunks.
	// --------------------------------------------------------

	var fileSize uint64

	for _, handle := range allHandles {

		data, err := s.readWholeChunk(handle.Id)
		if err != nil {
			return []uint64{}, 0, err
		}

		fileSize += uint64(len(data))
	}

	if offset > fileSize {
		return []uint64{}, 0, fmt.Errorf(
			"offset %d exceeds file size %d",
			offset,
			fileSize,
		)
	}

	if length > fileSize-offset {
		length = fileSize - offset
	}

	if length == 0 {
		return []uint64{}, 0, nil
	}

	// --------------------------------------------------------
	// Determine affected chunk.
	// --------------------------------------------------------

	chunkSize := uint64(config.ChunkSize)

	startChunkIndex := offset / chunkSize
	startChunkOffset := offset % chunkSize

	endDeleteOffset := offset + length

	// The first chunk containing bytes AFTER the deleted range.
	suffixChunkIndex := endDeleteOffset / chunkSize
	suffixOffset := endDeleteOffset % chunkSize

	fmt.Printf(
		"[Storage] delete range: offset=%d length=%d startChunk=%d startOffset=%d suffixChunk=%d suffixOffset=%d\n",
		offset,
		length,
		startChunkIndex,
		startChunkOffset,
		suffixChunkIndex,
		suffixOffset,
	)

	// --------------------------------------------------------
	// We only need data from startChunk onward.
	//
	// Prefix before offset stays exactly where it is.
	// --------------------------------------------------------

	var rebuilt []byte

	// --------------------------------------------------------
	// 1. Prefix of first affected chunk.
	//
	// Example:
	//
	// ABCDEFGHIJ
	//        ^
	//      offset=7
	//
	// Keep ABCDEFG.
	// --------------------------------------------------------

	startHandle := allHandles[startChunkIndex]

	startData, err := s.readWholeChunk(startHandle.Id)
	if err != nil {
		return []uint64{}, 0, err
	}

	if startChunkOffset > uint64(len(startData)) {
		return []uint64{}, 0, fmt.Errorf(
			"offset %d exceeds chunk %d size %d",
			startChunkOffset,
			startHandle.Id,
			len(startData),
		)
	}

	rebuilt = append(
		rebuilt,
		startData[:startChunkOffset]...,
	)

	// --------------------------------------------------------
	// 2. Suffix starts at:
	//
	//     endDeleteOffset
	//
	// We need everything from there to EOF.
	// --------------------------------------------------------

	if suffixChunkIndex < uint64(len(allHandles)) {

		// Case: suffix begins inside a chunk.
		suffixHandle := allHandles[suffixChunkIndex]

		suffixData, err := s.readWholeChunk(
			suffixHandle.Id,
		)
		if err != nil {
			return []uint64{}, 0, err
		}

		if suffixOffset > uint64(len(suffixData)) {
			return []uint64{}, 0, fmt.Errorf(
				"suffix offset %d exceeds chunk %d size %d",
				suffixOffset,
				suffixHandle.Id,
				len(suffixData),
			)
		}

		rebuilt = append(
			rebuilt,
			suffixData[suffixOffset:]...,
		)

		// ----------------------------------------------------
		// Read remaining chunks AFTER the suffix chunk.
		// ----------------------------------------------------

		for i := suffixChunkIndex + 1; i < uint64(len(allHandles)); i++ {

			data, err := s.readWholeChunk(
				allHandles[i].Id,
			)
			if err != nil {
				return []uint64{}, 0, err
			}

			rebuilt = append(
				rebuilt,
				data...,
			)
		}
	}
	fmt.Printf(
		"[Storage] rebuilt data = %q\n",
		string(rebuilt),
	)

	// --------------------------------------------------------
	// 3. Determine how many chunks are needed after deletion.
	// --------------------------------------------------------

	newChunkCount := 0

	if len(rebuilt) > 0 {
		newChunkCount =
			(len(rebuilt) + int(chunkSize) - 1) /
				int(chunkSize)
	}

	availableHandles :=
		len(allHandles) - int(startChunkIndex)

	if newChunkCount > availableHandles {
		return []uint64{}, 0, fmt.Errorf(
			"not enough existing chunk handles: need %d, have %d",
			newChunkCount,
			availableHandles,
		)
	}

	// --------------------------------------------------------
	// 4. Rewrite ONLY chunks from startChunk onward.
	//
	// Chunks before startChunk are untouched.
	// --------------------------------------------------------

	for i := 0; i < newChunkCount; i++ {

		handleIndex :=
			int(startChunkIndex) + i

		handle :=
			allHandles[handleIndex]

		start :=
			i * int(chunkSize)

		end :=
			start + int(chunkSize)

		if end > len(rebuilt) {
			end = len(rebuilt)
		}

		data := rebuilt[start:end]

		fmt.Printf(
			"[Storage] rewriting chunk %d with %q\n",
			handle.Id,
			string(data),
		)

		path := s.chunkPath(handle.Id)

		err := os.WriteFile(
			path,
			data,
			0644,
		)

		if err != nil {
			return []uint64{}, 0, err
		}

		s.chunks[handle.Id] =
			uint64(len(data))
	}

	// --------------------------------------------------------
	// 5. Delete trailing chunks that are no longer needed.
	// --------------------------------------------------------

	firstDeletedHandle :=
		int(startChunkIndex) + newChunkCount

	for i := firstDeletedHandle; i < len(allHandles); i++ {

		handle := allHandles[i]

		fmt.Printf(
			"[Storage] deleting chunk %d\n",
			handle.Id,
		)

		path := s.chunkPath(handle.Id)

		err := os.Remove(path)

		if err != nil && !os.IsNotExist(err) {
			return []uint64{}, 0, err
		}

		delete(s.chunks, handle.Id)
	}

	surviving := make([]uint64, 0)

	// First: chunks before the deletion.
	// These were never modified and must remain in metadata.
	for i := uint64(0); i < startChunkIndex; i++ {
		surviving = append(
			surviving,
			allHandles[i].Id,
		)
	}

	// Second: chunks that were rewritten after the deletion.
	for i := 0; i < newChunkCount; i++ {
		handleIndex := int(startChunkIndex) + i

		surviving = append(
			surviving,
			allHandles[handleIndex].Id,
		)
	}

	newSize := fileSize - length

	return surviving, newSize, nil
}
