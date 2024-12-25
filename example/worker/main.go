package main

import (
	"fmt"
	"log"
	"mapreduce"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// MapFunc implements word counting map functionality.
// It splits input text into words and emits each word with a count of 1.
func MapFunc(file string, value string) (res []mapreduce.KeyValue) {
	// Split input text into words
	words := strings.Fields(value)
	for _, word := range words {
		// Create a key-value pair for each word, with value "1"
		res = append(res, mapreduce.KeyValue{
			Key:   strings.ToLower(word), // Convert to lowercase to ignore case
			Value: "1",
		})
	}
	return
}

// ReduceFunc implements word counting reduce functionality.
// It counts the number of occurrences for each word.
func ReduceFunc(key string, values []string) string {
	// Return the count of values as a string
	return strconv.Itoa(len(values))
}

// runWorkerWithRetry starts the worker process with retry mechanism
func runWorkerWithRetry(masterSocket, workerSocket string, done chan struct{}) {
	const (
		maxRetries     = 5
		retryInterval  = time.Second * 2
		taskWaitPeriod = time.Second
	)

	for {
		for i := 0; i < maxRetries; i++ {
			if i > 0 {
				log.Printf("Retry attempt %d/%d...", i+1, maxRetries)
				time.Sleep(retryInterval)
			}

			err := mapreduce.RunWorker(masterSocket, workerSocket, MapFunc, ReduceFunc, -1)
			if err != nil {
				log.Printf("Worker error: %v", err)
				// Continue retrying for connection-related errors
				if strings.Contains(err.Error(), "connection") ||
					strings.Contains(err.Error(), "RPC") {
					continue
				}
				// For other errors, assume the task is complete
				log.Printf("Worker completed or encountered non-connection error")
				close(done)
				return
			}

			// If RunWorker returns normally, reset retry counter and wait for new tasks
			i = -1
			time.Sleep(taskWaitPeriod)
		}

		// Exit if max retries reached
		log.Printf("Worker failed after %d retries", maxRetries)
		close(done)
		return
	}
}

func main() {
	// Validate command line arguments
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <worker-number>\n", os.Args[0])
		os.Exit(1)
	}

	// Parse worker number from command line
	workerNum, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid worker number: %v", err)
	}

	// Configure worker paths
	masterSocket := mapreduce.Config["master_socket"]
	workerSocket := fmt.Sprintf("%s/worker-%d-%d.sock",
		mapreduce.Config["socket_base"],
		os.Getpid(),
		workerNum,
	)

	// Ensure socket directory exists
	socketDir := mapreduce.Config["socket_base"]
	if err := os.MkdirAll(socketDir, 0777); err != nil {
		log.Fatalf("Failed to create socket directory: %v", err)
	}

	// Clean up any existing socket file
	if err := os.Remove(workerSocket); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove old worker socket: %v", err)
	}

	// Print startup information
	log.Printf("Starting worker %d...", workerNum)
	log.Printf("Connecting to master at: %s", masterSocket)
	log.Printf("Worker socket: %s", workerSocket)

	// Setup signal handling
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// Start worker process in background
	go runWorkerWithRetry(masterSocket, workerSocket, done)

	// Wait for completion or interrupt
	select {
	case <-interrupt:
		log.Println("Received interrupt signal. Shutting down...")
	case <-done:
		log.Printf("Worker %d completed", workerNum)
	}

	log.Printf("Worker %d exiting", workerNum)
}
