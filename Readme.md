# Distributed MapReduce Framework

A distributed MapReduce implementation in Go, inspired by Google's MapReduce paper. This framework allows you to process large datasets across multiple machines using the MapReduce programming model.

## Features

- Distributed processing with master-worker architecture
- Fault tolerance with automatic retry mechanism
- Unix domain sockets for inter-process communication
- Configurable number of map and reduce tasks
- Easy-to-use interface for implementing custom map and reduce functions
- YAML-based configuration for easy deployment

## Project Structure

```
.
├── assets/
│   ├── input/    # Input files directory
│   ├── output/   # Intermediate output files
│   └── result/   # Final result files
├── example/
│   ├── master/   # Example master implementation
│   └── worker/   # Example worker implementation
├── config.yaml   # Configuration file
└── src/         # Core MapReduce implementation
```

## Getting Started

### Prerequisites

- Go 1.16 or later
- Unix-like operating system (Linux, macOS)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd mapreduce
```

2. Install dependencies:
```bash
go mod download
```

### Configuration

Edit `config.yaml` to configure paths and settings:

```yaml
paths:
  output: "./assets/output"
  input: "./assets/input"
  result: "./assets/result"
  socket_base: "/tmp/824-socket"
  master_socket: "/tmp/824-socket/master.sock"
```

### Running the Example

1. Start the master node:
```bash
cd example/master
go run main.go
```

2. Start one or more worker nodes in separate terminals:
```bash
cd example/worker
go run main.go 1  # Start worker 1
go run main.go 2  # Start worker 2 (in another terminal)
```

The example implements a word count application that:
- Counts word occurrences across multiple input files
- Handles case-insensitive word matching
- Outputs results in a sorted format

## Implementation Details

### Master Node

- Manages task distribution
- Handles worker registration and task assignment
- Coordinates map and reduce phases
- Monitors worker health and handles failures

### Worker Node

- Executes map and reduce tasks
- Automatically retries on connection failures
- Handles graceful shutdown
- Reports task completion to master

### MapReduce Process

1. **Map Phase**:
   - Input files are split among map tasks
   - Each mapper processes its chunk and emits key-value pairs
   - Intermediate results are written to disk

2. **Reduce Phase**:
   - Reducers collect and sort intermediate data
   - Values for each key are aggregated
   - Final results are written to output files

## Example Application: Word Count

The included example demonstrates word counting:

```go
// Map function splits text into words
func MapFunc(file string, value string) []KeyValue {
    words := strings.Fields(value)
    var res []KeyValue
    for _, word := range words {
        res = append(res, KeyValue{
            Key:   strings.ToLower(word),
            Value: "1",
        })
    }
    return res
}

// Reduce function counts word occurrences
func ReduceFunc(key string, values []string) string {
    return strconv.Itoa(len(values))
}
```

## Error Handling

- Automatic retry mechanism for transient failures
- Graceful shutdown on interruption
- Detailed logging for debugging
- Cleanup of temporary files and sockets

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by Google's MapReduce paper
- Based on MIT 6.824 Distributed Systems course
