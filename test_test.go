// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Test configuration constants
const (
	nNumber = 100 // Total number of input numbers to process
	nMap    = 10  // Number of map tasks to create
	nReduce = 5   // Number of reduce tasks to use
)

// MapFunc implements the map phase of the MapReduce job.
// It takes a file name and its content, and produces key-value pairs.
//
// Parameters:
//   - file: Name of the input file being processed
//   - value: Content of the input file as a string
//
// Returns:
//   - []KeyValue: Slice of key-value pairs generated from the input
func MapFunc(file string, value string) (res []KeyValue) {
	// Split input into lines and process each non-empty line
	lines := strings.Split(value, "\n")
	for _, line := range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		// Create key-value pair for each number
		kv := KeyValue{line, "1"}
		res = append(res, kv)
	}
	return
}

// ReduceFunc implements the reduce phase of the MapReduce job.
// It counts the occurrences of each key by counting the values.
//
// Parameters:
//   - key: The key to process
//   - values: Slice of values associated with the key
//
// Returns:
//   - string: Count of values converted to string
func ReduceFunc(key string, values []string) string {
	// Count occurrences
	count := len(values)
	return fmt.Sprintf("%d", count)
}

// makeInputs creates test input files with sequential numbers.
// It distributes nNumber numbers across num files evenly.
//
// Parameters:
//   - num: Number of input files to create
//
// Returns:
//   - []string: Slice of created file paths
//
// The function creates files in the ./assets/input directory,
// each containing a portion of the numbers from 0 to nNumber-1.
func makeInputs(num int) []string {
	var names []string
	i := 0
	for f := 0; f < num; f++ {
		// Create file name following MIT6.824 convention
		names = append(names, fmt.Sprintf("./assets/input/824-mrinput-%d.txt", f))

		file, err := os.Create(names[f])
		if err != nil {
			log.Fatalf("create input file %s failed. error: %v", names[f], err)
		}
		w := bufio.NewWriter(file)

		// Write sequential numbers to the file
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// init creates all necessary directories for the test environment.
// It ensures a clean state by removing and recreating directories.
func init() {
	// Use paths from the Config variable
	dirs := []string{
		Config["output"],
		Config["input"],
		Config["socket_base"],
	}

	// Ensure all necessary directories exist
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("Failed to remove directory %s: %v", dir, err)
		}
		if err := os.MkdirAll(dir, 0777); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}
}

// setup prepares the test environment by creating a master node
// and generating input files.
//
// Returns:
//   - *Master: Configured master node ready for testing
func setup() *Master {
	fmt.Printf("Setup Master\n")
	files := makeInputs(nMap)

	socketPath := "/tmp/824-socket/master.sock"
	os.Remove(socketPath) // Clean up any existing socket file

	mr := Distributed("test", files, nReduce, socketPath)
	return mr
}

// workerFlag generates a unique socket path for a worker.
//
// Parameters:
//   - num: Worker number for uniqueness
//
// Returns:
//   - string: Unix domain socket path for the worker
func workerFlag(num int) string {
	return fmt.Sprintf("/tmp/824-socket/worker-%d-%d.sock", os.Getpid(), num)
}

// TestBasic runs a basic end-to-end test of the MapReduce framework.
// It sets up a master and two workers, processes input files, and
// verifies the results.
func TestBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	timeout := time.After(2 * time.Minute)
	mr := setup()
	defer func() {
		mr.Shutdown(new(struct{}), new(struct{}))
		os.RemoveAll("/tmp/824-socket")
	}()

	// Start two worker processes
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, workerFlag(i), MapFunc, ReduceFunc, -1)
	}

	// Wait for job completion or timeout
	done := make(chan struct{})
	go func() {
		mr.Wait()
		close(done)
	}()

	select {
	case <-done:
		checkResults(t)
	case <-timeout:
		t.Fatal("Test timed out")
	}
}

// checkResults verifies the output of the MapReduce job.
// It ensures that all numbers were processed correctly.
//
// The verification process:
// 1. Reads the result file
// 2. Parses each line to extract numbers and their counts
// 3. Verifies that each number from 0 to nNumber-1 appears exactly once
// 4. Checks that no unexpected numbers are present
func checkResults(t *testing.T) {
	// Open and read the result file
	resultFile := "./assets/result/mrt.result.txt"
	file, err := os.Open(resultFile)
	if err != nil {
		t.Fatalf("Failed to open result file: %v", err)
	}
	defer file.Close()

	// Create a map to track seen numbers
	seen := make(map[int]bool)
	scanner := bufio.NewScanner(file)

	// Process each line in the result file
	for scanner.Scan() {
		line := scanner.Text()
		// Parse line format "number: [count]"
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			t.Errorf("Invalid line format: %s", line)
			continue
		}

		// Extract and validate the number
		num, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			t.Errorf("Invalid number format: %s", parts[0])
			continue
		}

		// Verify number is in valid range
		if num < 0 || num >= nNumber {
			t.Errorf("Number out of range: %d", num)
			continue
		}

		// Check for duplicates
		if seen[num] {
			t.Errorf("Duplicate number found: %d", num)
			continue
		}
		seen[num] = true

		// Verify the count is 1 (each number should appear exactly once)
		count := strings.TrimSpace(parts[1])
		if count != "[1]" {
			t.Errorf("Unexpected count for number %d: %s", num, count)
		}
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error reading result file: %v", err)
	}

	// Verify all numbers were found
	for i := 0; i < nNumber; i++ {
		if !seen[i] {
			t.Errorf("Missing number: %d", i)
		}
	}

	// Verify no extra numbers were found
	if len(seen) > nNumber {
		t.Errorf("Found more numbers than expected: got %d, want %d", len(seen), nNumber)
	}
}
