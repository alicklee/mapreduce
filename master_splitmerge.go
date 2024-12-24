// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
)

// merge combines the results from all reduce tasks into a single output file.
// This is the final phase of the MapReduce job that aggregates all the results.
//
// The merge process:
// 1. Creates the result directory if it doesn't exist
// 2. Reads all intermediate files from reduce tasks
// 3. Combines values for each key from all reducers
// 4. Sorts the keys for deterministic output
// 5. Writes the final results to a single output file
//
// Error handling:
//   - Logs and continues if result directory creation fails
//   - Logs and continues if any intermediate file cannot be opened
//   - Logs and returns if final output file cannot be created
//
// The output format:
//   - Each line contains a key and its associated values
//   - Keys are sorted alphabetically
//   - Values are preserved in the order they were produced
func (mr *Master) merge() {
	// Create result directory with all necessary parent directories
	resultDir := "./assets/result"
	if err := os.MkdirAll(resultDir, 0777); err != nil {
		log.Printf("Failed to create result directory: %v", err)
		return
	}

	// Map to store all key-value pairs from reduce tasks
	// The slice of strings for each key preserves the order of values
	result := make(map[string][]string)

	// Read and process each reduce task's output file
	for i := 0; i < mr.nReduce; i++ {
		fileName := mergeName(mr.jobName, i)
		fmt.Printf("Merge: reading %s\n", fileName)

		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("Merge: failed to open file %s: %v\n", fileName, err)
			continue
		}

		// Decode JSON-encoded key-value pairs
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break // End of file or error
			}
			// Append each value to its key's slice
			result[kv.Key] = append(result[kv.Key], kv.Value)
		}
		file.Close()
	}

	// Create the final output file
	resultFile := filepath.Join(resultDir, "mrt.result.txt")
	file, err := os.Create(resultFile)
	if err != nil {
		log.Printf("Merge: failed to create result file: %v\n", err)
		return
	}
	defer file.Close()

	// Use buffered writer for efficient output
	w := bufio.NewWriter(file)
	defer w.Flush()

	// Sort keys for deterministic output order
	var keys []string
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Write each key and its values to the output file
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %v\n", k, result[k])
	}
}
