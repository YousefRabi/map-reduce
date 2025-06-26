# Distributed MapReduce System

A distributed MapReduce implementation built in Go as part of MIT 6.5840 (Distributed Systems) coursework.

## Overview

This implementation consists of two main components:
- **Coordinator**: Manages task distribution, handles worker failures, and coordinates the overall MapReduce job execution
- **Worker**: Executes map and reduce tasks, communicates with the coordinator via RPC, and handles file I/O operations

The system is designed to handle worker failures gracefully by reassigning tasks and supports parallel execution of both map and reduce phases.

## Architecture

### Key Components

- **Coordinator (`mr/coordinator.go`)**: 
  - Distributes map and reduce tasks to available workers
  - Tracks task progress and handles timeouts (10-second worker timeout)
  - Manages job completion and cleanup
  - Thread-safe with proper synchronization

- **Worker (`mr/worker.go`)**: 
  - Requests tasks from coordinator in a continuous loop
  - Executes map functions on input files, producing intermediate key-value pairs
  - Executes reduce functions on grouped intermediate data
  - Uses JSON encoding for intermediate file storage

- **RPC Interface (`mr/rpc.go`)**: 
  - Defines communication protocol between coordinator and workers
  - Handles task assignment, progress reporting, and job completion

### File Organization

```
src/
├── main/
│   ├── mrcoordinator.go    # Coordinator main program
│   ├── mrworker.go         # Worker main program
│   ├── mrsequential.go     # Sequential reference implementation
│   ├── test-mr.sh          # Test script
│   └── pg-*.txt            # Sample input files
├── mr/
│   ├── coordinator.go     # Coordinator implementation
│   ├── worker.go          # Worker implementation
│   └── rpc.go             # RPC definitions
└── mrapps/
    ├── wc.go              # Word count application
    ├── indexer.go         # Text indexer application
    └── crash.go           # Crash testing application
```

## Features

- **Fault Tolerance**: Automatic task reassignment when workers fail or timeout
- **Parallel Processing**: Concurrent execution of map and reduce tasks
- **Flexible Applications**: Plugin-based architecture for different MapReduce applications
- **Intermediate File Management**: Proper handling of intermediate files with naming convention `mr-X-Y`
- **Atomic Operations**: Safe file operations using temporary files and atomic renames

## Getting Started

### Prerequisites

- Go 1.24.4 or later
- Unix-like environment (Linux, macOS)

### Building and Running

1. **Build a MapReduce application** (e.g., word count):
   ```bash
   cd src/main
   go build -buildmode=plugin ../mrapps/wc.go
   ```

2. **Start the coordinator** with input files:
   ```bash
   go run mrcoordinator.go pg-*.txt
   ```

3. **Start one or more workers** in separate terminals:
   ```bash
   go run mrworker.go wc.so
   ```

4. **View results** in output files:
   ```bash
   cat mr-out-* | sort
   ```

### Example: Word Count

```bash
# Build the word count plugin
cd src/main
go build -buildmode=plugin ../mrapps/wc.go

# Clean previous outputs
rm -f mr-out-*

# Start coordinator (in one terminal)
go run mrcoordinator.go pg-*.txt

# Start workers (in other terminals)
go run mrworker.go wc.so
go run mrworker.go wc.so

# Check results
cat mr-out-* | sort | head -20
```

## Testing

Run the comprehensive test suite:

```bash
cd src/main
bash test-mr.sh
```

For stress testing:
```bash
bash test-mr-many.sh 10  # Run tests 10 times
```

The test suite validates:
- Correctness of word count and indexer applications
- Map and reduce task parallelism
- Worker crash recovery
- Job completion handling
- Output file format compliance

## Implementation Details

### Task Distribution Strategy

- **Map Phase**: Each input file becomes one map task
- **Reduce Phase**: Intermediate files are partitioned into `nReduce` buckets using hash partitioning
- **Intermediate Files**: Named as `mr-X-Y` where X is map task number, Y is reduce task number

### Fault Tolerance Mechanism

- Workers that don't complete tasks within 10 seconds are considered failed
- Failed tasks are automatically reassigned to other workers
- System continues operation even with worker failures

### Synchronization

- Coordinator uses mutex locks to protect shared state
- Channel-based communication for task coordination
- Atomic file operations prevent corruption from crashes

## Applications Included

- **Word Count** (`wc.go`): Counts word occurrences in text files
- **Indexer** (`indexer.go`): Creates an inverted index of words to documents
- **Crash Test** (`crash.go`): Randomly crashes to test fault tolerance

## References

- [Original MapReduce Paper](https://research.google.com/archive/mapreduce.html) by Dean & Ghemawat
- [MIT 6.5840 Course Materials](https://pdos.csail.mit.edu/6.824/)
