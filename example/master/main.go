package main

import (
	"fmt"
	"log"
	"mapreduce"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// JobParse is the type alias for mapreduce.JobParse
type JobParse = mapreduce.JobParse

// setupInputFiles creates example input files for word counting
func setupInputFiles() (string, string, error) {
	// Get project root directory
	rootDir, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("failed to get working directory: %v", err)
	}

	// Create input directory path
	inputDir := filepath.Join(rootDir, strings.TrimPrefix(mapreduce.Config["input"], "./"))

	// Ensure input directory exists
	if err := os.MkdirAll(inputDir, 0777); err != nil {
		return "", "", fmt.Errorf("failed to create input directory: %v", err)
	}

	// Create example input files
	inputFile1 := filepath.Join(inputDir, "example1.txt")
	inputFile2 := filepath.Join(inputDir, "example2.txt")

	// Sample text content for word counting
	content1 := `The quick brown fox jumps over the lazy dog
A quick brown dog jumps over the lazy fox
The lazy dog and fox are quick to jump`

	content2 := `Brown foxes and dogs are all quick and lazy
The quick brown fox likes to jump and play
Dogs and foxes are natural enemies but can be friends`

	// Write content to files
	if err := os.WriteFile(inputFile1, []byte(content1), 0666); err != nil {
		return "", "", fmt.Errorf("failed to write input file 1: %v", err)
	}
	if err := os.WriteFile(inputFile2, []byte(content2), 0666); err != nil {
		return "", "", fmt.Errorf("failed to write input file 2: %v", err)
	}

	return inputFile1, inputFile2, nil
}

// setupMasterSocket prepares the socket directory and cleans up old socket files
func setupMasterSocket() error {
	socketDir := mapreduce.Config["socket_base"]
	masterSocket := mapreduce.Config["master_socket"]

	// Ensure socket directory exists
	if err := os.MkdirAll(socketDir, 0777); err != nil {
		return fmt.Errorf("failed to create socket directory: %v", err)
	}

	// Clean up any existing socket file
	if err := os.Remove(masterSocket); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove old master socket: %v", err)
	}

	return nil
}

func main() {
	// Setup input files for word counting
	inputFile1, inputFile2, err := setupInputFiles()
	if err != nil {
		log.Fatalf("Failed to setup input files: %v", err)
	}

	log.Println("Input files created:")
	log.Println(inputFile1)
	log.Println(inputFile2)

	// Initialize master node
	log.Println("Starting master node...")

	// Configure MapReduce task
	inputFiles := []string{inputFile1, inputFile2}
	nReduce := len(inputFiles)                        // Number of reduce tasks
	masterSocket := mapreduce.Config["master_socket"] // Master socket path

	// Setup socket directory and cleanup
	if err := setupMasterSocket(); err != nil {
		log.Fatalf("Failed to setup master socket: %v", err)
	}

	// Print configuration information
	log.Printf("Master socket: %s", masterSocket)
	log.Printf("Number of reduce tasks: %d", nReduce)
	log.Printf("Input files: %v", inputFiles)

	// Create and start master
	log.Println("Creating and starting master...")
	master := mapreduce.Distributed(JobParse("wordcount"), inputFiles, nReduce, masterSocket)
	if master == nil {
		log.Fatal("Failed to create master")
	}

	log.Println("Waiting for workers to connect...")

	// Setup signal handling for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// Wait for task completion in background
	done := make(chan struct{})
	go func() {
		master.Wait()
		close(done)
	}()

	// Wait for completion or interrupt
	select {
	case <-done:
		log.Println("All tasks completed successfully")
	case <-interrupt:
		log.Println("Received interrupt signal. Shutting down...")
		// Give master time to cleanup
		time.Sleep(time.Second)
	}

	log.Println("Master node completed")
	log.Println("Results can be found in: ./assets/result/mrt.result.txt")
}
