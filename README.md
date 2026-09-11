# Mini-GFS

A distributed, fault-tolerant file system inspired by the architecture and design principles of Google File System (GFS).

Mini-GFS separates metadata management from data storage using a centralized **Master** and multiple **ChunkServers**. The Master manages the filesystem namespace, chunk metadata, placement, replication, and primary assignments, while ChunkServers store the actual file data.

Clients communicate with the Master for metadata and then communicate directly with ChunkServers for data operations, preventing the Master from becoming a data-transfer bottleneck.

---

## Architecture

```text
                         ┌─────────────────┐
                         │     Client      │
                         └────────┬────────┘
                                  │
                         Metadata RPCs
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     Master      │
                         │                 │
                         │ • Namespace     │
                         │ • Chunk Metadata│
                         │ • Placement     │
                         │ • Replication   │
                         │ • Leases        │
                         │ • Failure       │
                         │   Detection     │
                         └────────┬────────┘
                                  │
                         Chunk Locations
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     Client      │
                         └────────┬────────┘
                                  │
                            Actual Data I/O
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
       ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
       │ ChunkServer 1│    │ ChunkServer 2│    │ ChunkServer 3│
       └──────────────┘    └──────────────┘    └──────────────┘
```

### Core Principle

The **Master handles metadata and coordination**, while **ChunkServers handle actual file data**.

```text
Client ── metadata ──► Master

Client ── data ──────► ChunkServers
```

This keeps the Master out of the main data-transfer path.

---

## Features

- Distributed chunk-based file storage
- Centralized metadata management
- 3-way chunk replication
- Primary-based writes
- Synchronous replica writes
- Direct Client-to-ChunkServer data transfer
- Parallel chunk reads
- Parallel writes to independent chunks
- File creation
- File read
- File write
- File append
- File truncation
- File deletion
- Heartbeat-based failure detection
- Automatic re-replication
- Primary failover
- Persistent Master metadata
- Persistent ChunkServer storage
- Restart recovery
- gRPC communication using Protocol Buffers
- Concurrent request handling using Go
- End-to-end failure and correctness testing

---

# Project Structure

```text
mini-gfs/
│
├── cmd/
│   ├── master/
│   │   └── main.go
│   ├── chunkserver/
│   │   └── main.go
│   └── client/
│       └── main.go
│
├── internal/
│   ├── master/
│   │   ├── metadata.go
│   │   ├── server.go
│   │   ├── replication.go
│   │   └── lease.go
│   │
│   ├── chunkserver/
│   │   ├── server.go
│   │   └── storage.go
│   │
│   ├── client/
│   │   └── client.go
│   │
│   ├── config/
│   │   └── constants.go
│   │
│   └── pb/
│       ├── master.pb.go
│       ├── master_grpc.pb.go
│       ├── chunk.pb.go
│       ├── chunk_grpc.pb.go
│       └── common.pb.go
│
├── proto/
│   ├── master.proto
│   ├── chunk.proto
│   └── common.proto
│
├── scripts/
│   └── generate_proto.sh
│
├── data/
│   └── <runtime ChunkServer storage>
│
├── metadata.json
├── go.mod
└── go.sum
```

### Component Responsibilities

| File | Responsibility |
|---|---|
| `internal/master/metadata.go` | Filesystem metadata, chunk mappings and ChunkServer state |
| `internal/master/server.go` | Master gRPC API and filesystem operations |
| `internal/master/replication.go` | Replica health, re-replication and recovery |
| `internal/master/lease.go` | Primary/lease management and failover |
| `internal/chunkserver/server.go` | ChunkServer gRPC API and replication forwarding |
| `internal/chunkserver/storage.go` | Local chunk storage and disk persistence |
| `internal/client/client.go` | Client filesystem operations and chunk-level I/O |
| `internal/config/constants.go` | System configuration |
| `proto/*.proto` | gRPC service and message definitions |
| `internal/pb/*` | Generated Protocol Buffer and gRPC code |
| `cmd/*/main.go` | Master, ChunkServer and Client entry points |

---

# Chunk Model

Files are divided into fixed-size chunks.

Each chunk has a unique handle and is stored on multiple ChunkServers.

The default replication factor is **3**:

```text
                  Chunk
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Primary   Replica   Replica
         CS1       CS2       CS3
```

One server acts as the **primary** and the remaining servers store replicas.

The Master maintains the mapping between files, chunks, and their locations.

---

# File Creation

When a Client creates a file:

```text
Client
   │
   │ CreateFile(path)
   ▼
Master
   │
   ├── Create namespace entry
   └── Initialize file metadata
   │
   ▼
Client
```

The file initially has no data chunks. Chunks are allocated as data is written.

---

# Write

A write consists of two stages.

### 1. Metadata stage

```text
Client
   │
   │ WriteFile(path, offset, length)
   ▼
Master
   │
   ├── Determine required chunks
   ├── Allocate chunks if required
   ├── Determine chunk locations
   └── Determine primary servers
   │
   ▼
Client
```

### 2. Data stage

The Client communicates directly with the relevant ChunkServers.

For each chunk:

```text
Client
   │
   │ WriteChunk
   ▼
Primary
   │
   ├──────────────► Replica 1
   │
   └──────────────► Replica 2
```

The primary coordinates the write and forwards the data to its replicas.

---

# Parallel Writes

Independent chunks can be written concurrently.

For example:

```text
Sequential:

Chunk 1 ─────►
              Chunk 2 ─────►
                            Chunk 3 ─────►
                                          Chunk 4 ─────►


Parallel:

Chunk 1 ─────►
Chunk 2 ─────►
Chunk 3 ─────►
Chunk 4 ─────►
```

The Client uses Go concurrency to dispatch independent chunk writes in parallel.

The Client waits for the required operations to complete before returning the
overall result.

Replication within an individual chunk remains coordinated through its primary.

---

# Read

A read also separates metadata from data transfer.

```text
Client
   │
   │ ReadFile(path, offset, length)
   ▼
Master
   │
   │ Return chunk locations
   ▼
Client
```

The Client then reads the required chunks directly from ChunkServers.

For multi-chunk reads:

```text
                 Client
              /    |    \
             ▼     ▼     ▼
          Chunk 1 Chunk 2 Chunk 3
             │      │      │
             ▼      ▼      ▼
            CS1    CS2    CS3
```

Independent chunk reads are performed concurrently.

After the reads complete, the Client reconstructs the requested byte range in
the correct file order.

---

# Append

Appending uses the same chunk-oriented write mechanism.

The Master determines the current end of the file and returns the required
chunk locations.

The Client writes the appended data starting at the correct offset.

If the final existing chunk has space, the append begins within that chunk;
remaining data is written to subsequent chunks.

---

# Truncate

Truncation reduces a file to a specified size.

```text
Before:

Chunk 1 | Chunk 2 | Chunk 3 | Chunk 4
                       ↑
                  New file end


After:

Chunk 1 | Chunk 2 | Partial Chunk 3
```

Affected chunks are truncated on the primary and all replicas.

Chunks completely beyond the new file size are deleted from all available
copies.

The Master metadata is updated to reflect the new file size and chunk layout.

---

# Delete

Deleting a file requires both metadata cleanup and physical chunk deletion.

```text
Client
   │
   │ DeleteFile(path)
   ▼
Master
   │
   │ Find all chunks
   ▼
Client / ChunkServers
   │
   ├── Delete primary chunk
   ├── Delete replica 1
   └── Delete replica 2
```

The file is removed from the Master namespace and its physical chunk copies
are removed from the ChunkServers.

---

# Failure Detection

ChunkServers periodically send heartbeats to the Master.

```text
ChunkServer ── heartbeat ──► Master
```

If a ChunkServer misses the configured number of heartbeats, the Master marks
it unavailable.

This triggers replication recovery for affected chunks.

```text
ChunkServer failure
        │
        ▼
Heartbeat timeout
        │
        ▼
Master marks server unavailable
        │
        ▼
Replication Manager
        │
        ▼
Find under-replicated chunks
        │
        ▼
Create replacement replicas
```

---

# Replication and Recovery

Under normal operation, each chunk has three healthy copies.

For example:

```text
Chunk 10

CS1 ── Primary
CS3 ── Replica
CS4 ── Replica
```

If CS4 fails:

```text
Chunk 10

CS1 ── Primary
CS3 ── Replica
CS4 ── FAILED
```

The ReplicationManager detects that only two healthy copies remain.

It selects an available ChunkServer that does not already contain the chunk:

```text
CS1 ── Primary
CS3 ── Replica
CS5 ── New Replica
```

The chunk is copied to the new server and the Master metadata is updated.

---

# Primary Failover

If the failed ChunkServer was the primary, a healthy replica can be promoted.

```text
Before:

Chunk 8
 ├── CS1 [PRIMARY]
 ├── CS2 [REPLICA]
 └── CS3 [REPLICA]


CS1 fails


After:

Chunk 8
 ├── CS1 [FAILED]
 ├── CS2 [PRIMARY]
 └── CS3 [REPLICA]
```

This allows subsequent writes to continue without requiring the failed primary
to recover first.

---

# Persistence

Both metadata and chunk data are persistent.

### Master

Master metadata is stored in:

```text
metadata.json
```

This allows filesystem metadata to survive Master restarts.

### ChunkServers

Chunk data is stored on disk:

```text
data/
├── chunkserver-1/
│   ├── <chunk>.chunk
│   └── ...
├── chunkserver-2/
│   └── ...
└── chunkserver-3/
    └── ...
```

ChunkServer startup scans its storage directory and reconstructs its local
chunk state.

Therefore, chunk contents survive process restarts.

> `data/` contains runtime-generated storage and should generally not be
> committed to the repository.

---

# Restart Recovery

```text
                 Process Restart
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
       Master                   ChunkServer
          │                         │
          ▼                         ▼
 Load metadata.json          Scan chunk files
          │                         │
          └────────────┬────────────┘
                       ▼
                   Heartbeats
                       │
                       ▼
              Cluster state restored
                       │
                       ▼
              Re-replication if needed
```

If chunks become under-replicated after recovery, the ReplicationManager can
restore the required replication factor.

---

# Concurrency and Race Safety

Concurrency exists at several levels.

### Client

Independent chunk reads and writes are dispatched concurrently.

### ChunkServer

Storage operations are synchronized so concurrent requests do not corrupt
shared local chunk state.

### Master

Metadata, replication state, and ChunkServer state require synchronization
because multiple clients and ChunkServers can interact with the Master
concurrently.

The system therefore separates:

- Metadata synchronization
- Storage synchronization
- Network communication
- Parallel chunk operations

---

# Protocol

Communication uses:

- **Go**
- **gRPC**
- **Protocol Buffers**

Protocol definitions:

```text
proto/
├── master.proto
├── chunk.proto
└── common.proto
```

Generated Go code:

```text
internal/pb/
```

Regenerate the Protocol Buffer code using:

```bash
./scripts/generate_proto.sh
```

---

# Testing

The system has been tested for both normal filesystem correctness and
distributed failure scenarios.

## Basic Operations

Tests cover:

- Create
- Write
- Read
- Append
- Truncate
- Delete

Tests verify both returned data and metadata.

Example:

```text
Expected:
"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

Actual:
"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

DATA + METADATA VERIFIED
```

## Replication Tests

Replication testing includes:

1. Creating multi-chunk files.
2. Verifying three copies per chunk.
3. Stopping a ChunkServer.
4. Waiting for heartbeat failure detection.
5. Detecting under-replicated chunks.
6. Automatically creating replacement replicas.
7. Promoting a replica when the primary fails.
8. Reading the recovered file.
9. Verifying data integrity.

## Persistence Tests

Persistence testing verifies:

- Metadata survives Master restart.
- Chunk data survives ChunkServer restart.
- File contents remain unchanged.
- Metadata file sizes remain consistent with actual data.
- Chunk state can be recovered after restart.

## Parallel I/O Tests

Multi-chunk files are used to verify concurrent reads and writes.

Example:

```text
========================================
PARALLEL I/O LATENCY TEST
========================================

Chunk size: 10
File size: 50
Expected chunks: 5

Writing large file...

Reading file...

Bytes read: 50
Data verified: OK
```

Parallel reads have demonstrated low latency during local testing.

Parallel writes are dispatched concurrently across independent chunks.

Current testing has identified an approximately 40-second latency in some
write configurations. Instrumentation indicates that Master metadata RPCs and
Client-to-ChunkServer connection establishment are fast, while the delay
occurs inside individual `WriteChunk` RPCs.

This is a known performance investigation area and does not change the
correctness of the parallel dispatch design.

---

# Technology Stack

| Category | Technology |
|---|---|
| Language | Go |
| RPC | gRPC |
| Serialization | Protocol Buffers |
| Concurrency | Goroutines and synchronization primitives |
| Storage | Local filesystem |
| Metadata Persistence | JSON |
| Communication | gRPC over TCP |
| Architecture | Distributed Client–Master–ChunkServer |

---

# Running the Project

## 1. Generate Protocol Buffers

```bash
./scripts/generate_proto.sh
```

## 2. Start the Master

```bash
go run ./cmd/master
```

## 3. Start ChunkServers

Start multiple ChunkServer instances using their respective configurations.

```bash
go run ./cmd/chunkserver
```

## 4. Start the Client

```bash
go run ./cmd/client
```

The Client can then perform filesystem operations through the implemented
Client API.

---

# Future Improvements

Potential improvements include:

- More sophisticated ChunkServer load balancing
- Improved replica selection
- Better timeout and retry handling
- More comprehensive performance benchmarking
- More granular replication scheduling
- Improved recovery from simultaneous node failures
- Additional concurrency stress testing
- More extensive fault-injection testing
- Production-oriented configuration management

Load balancing is currently considered a future improvement rather than a
completed feature.

---

# Project Goals

Mini-GFS was developed as an end-to-end exploration of practical distributed
systems concepts.

The project demonstrates:

- Distributed storage
- Metadata management
- Chunk-based storage
- Replication
- Failure detection
- Fault tolerance
- Primary failover
- Persistence
- Restart recovery
- Concurrent I/O
- RPC-based service communication
- Distributed state management

Rather than being only a simulation, Mini-GFS implements an actual Client,
Master, ChunkServer storage layer, RPC interface, persistent storage,
replication manager, failure detection, and recovery mechanisms.
