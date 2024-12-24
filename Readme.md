# MapReduce Framework

A distributed MapReduce implementation in Go, inspired by MIT 6.824 distributed systems course.

## Features

- Distributed processing with master-worker architecture
- Automatic task distribution and failure handling
- Support for custom map and reduce functions
- Fault tolerance with task retry mechanism
- Efficient result merging and sorting

## Usage Example

Here's a simple example that counts word occurrences in text files:

```go
package main

import (
    "strings"
    "github.com/alicklee/mapreduce"
)

// MapFunc splits text into words and emits (word, "1") pairs
func MapFunc(file string, content string) []mapreduce.KeyValue {
    words := strings.Fields(content)
    var results []mapreduce.KeyValue
    for _, word := range words {
        results = append(results, mapreduce.KeyValue{
            Key:   word,
            Value: "1",
        })
    }
    return results
}

// ReduceFunc counts occurrences of each word
func ReduceFunc(key string, values []string) string {
    return fmt.Sprintf("%d", len(values))
}

func main() {
    // Configure the job
    inputFiles := []string{"file1.txt", "file2.txt"}
    nReduce := 5
    
    // Create and start the master
    master := mapreduce.Distributed(
        "word-count",  // job name
        inputFiles,    // input files
        nReduce,       // number of reduce tasks
        "master.sock", // master socket path
    )
    
    // Start some workers
    for i := 0; i < len(inputFiles); i++ {
        go mapreduce.RunWorker(
            master.Address(),           // master address
            fmt.Sprintf("worker%d", i), // worker ID
            MapFunc,                    // map function
            ReduceFunc,                 // reduce function
            -1,                        // number of tasks (use -1 for unlimited)
        )
    }
    
    // Wait for completion
    master.Wait()
}
```

### Input Files
Create text files in the `./assets/input/` directory:
```
file1.txt:
hello world
hello mapreduce

file2.txt:
world of
mapreduce
```

### Output
The results will be written to `./assets/result/mrt.result.txt`:
```
hello: [2]
mapreduce: [2]
of: [1]
world: [2]
```

## Running Tests

```bash
# Run all tests
go test

# Run specific test
go test -run TestBasic

# Run with timeout
go test -timeout 2m
```

## Implementation Details

1. **Map Phase**: 
   - Splits input into tasks
   - Processes each file independently
   - Generates intermediate key-value pairs

2. **Reduce Phase**:
   - Collects intermediate results
   - Groups values by key
   - Applies reduce function to each group

3. **Merge Phase**:
   - Combines all reduce outputs
   - Sorts results by key
   - Writes final output file

## Error Handling

- Automatic retry for failed tasks
- Exponential backoff for retries
- Graceful worker failure handling
- Proper cleanup of temporary files

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
